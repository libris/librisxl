package whelk.housekeeping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import groovy.lang.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.IdGenerator;
import whelk.JsonLd;
import whelk.Whelk;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static whelk.util.Jackson.mapper;

public class NotificationGenerator extends HouseKeeper {

    private static final Logger log = LogManager.getLogger(NotificationGenerator.class);

    public static final String STATE_KEY = "CXZ notification generator";
    private static final int MAX_OBSERVATIONS_PER_CHANGE = 20;
    private String status = "OK";
    private final Whelk whelk;

    private static final List<String> INSTANCE_PROPERTIES_TO_EMBELLISH = List.of(
            "mainEntity",
            "instanceOf",
            "contribution",
            "hasTitle",
            "intendedAudience",
            "classification",
            "precededBy",
            "succeededBy",
            "contribution",
            "agent",
            "isPartOf",
            "subject",
            "continuedBy",
            "continues"
    );

    public NotificationGenerator(Whelk whelk) {
        this.whelk = whelk;
    }

    @Override
    public String getName() {
        return "Notifications generator";
    }

    @Override
    public String getStatusDescription() {
        return status;
    }

    @Override
    public String getCronSchedule() {
        return "* * * * *";
    }

    @Override
    public void trigger() {
        // Determine the time interval of changes for which to generate notifications.
        Instant now = Instant.now();
        Timestamp from = Timestamp.from(now); // First run? Default to now (=do nothing but set the timestamp for next run)
        Map state = whelk.getStorage().getState(STATE_KEY);
        if (state != null && state.get("lastGenerationTime") != null)
            from = Timestamp.from(ZonedDateTime.parse(
                    (String) state.get("lastGenerationTime"), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant());
        Timestamp until = Timestamp.from(now);

        Connection connection = whelk.getStorage().getOuterConnection();
        try {
            connection.setAutoCommit(false);

            // Fetch all changed IDs within the interval
            String sql = "SELECT id, ARRAY_AGG(data#>>'{@graph,0,hasChangeNote}') as changeNotes FROM lddb__versions WHERE collection IN ('bib', 'auth') AND deleted = false AND ( modified > ? AND modified <= ? ) group by id;";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, from);
                statement.setTimestamp(2, until);
                statement.setFetchSize(512);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String id = resultSet.getString("id");

                        List<Document> resultingChangeObservations = new ArrayList<>();

                        Array changeNotesArray = resultSet.getArray("changeNotes");
                        List<Object> changeNotes = new ArrayList<>();
                        for (Object o : (Object[]) changeNotesArray.getArray()) {
                            if (o != null)
                                changeNotes.add(o);
                        }

                        Map<String, List<Object>> changedInstanceIDsWithComments = new LinkedHashMap<>();

                        List<Tuple2<String, String>> dependers =
                                new ArrayList<>(whelk.getStorage().followDependers(id, List.of("itemOf")));
                        dependers.add(new Tuple2<>(id, null)); // This ID too, not _only_ the dependers!
                        for (Tuple2<String, String> depender : dependers) {
                            String dependerID = depender.getV1();
                            Document dependerDocument = whelk.getStorage().load(dependerID);

                            // Filter out certain groups of instances, which we do not want observations for
                            boolean filtered = false;
                            String dependerMainEntityType = dependerDocument.getThingType();
                            if (dependerMainEntityType == null)
                                filtered = true;
                            else if (dependerMainEntityType.equals("Electronic"))
                                filtered = true;
                            String encodingLevel = dependerDocument.getEncodingLevel();
                            if (encodingLevel == null)
                                filtered = true;
                            else {
                                if (encodingLevel.equals("marc:PartialPreliminaryLevel")
                                        || encodingLevel.equals("marc:PrepublicationLevel")) {
                                    filtered = true;
                                }
                            }

                            if (dependerMainEntityType != null
                                    && whelk.getJsonld().isSubClassOf(dependerMainEntityType, "Instance") && !filtered) {
                                changedInstanceIDsWithComments
                                        .computeIfAbsent(dependerID, f -> new ArrayList<>())
                                        .addAll(changeNotes);
                            }
                        }

                        for (String instanceId : changedInstanceIDsWithComments.keySet()) {
                            try {
                                resultingChangeObservations.addAll(generateObservationsForAffectedInstance(
                                        instanceId, changedInstanceIDsWithComments.get(instanceId),
                                        from.toInstant(), until.toInstant()));
                            } catch (Throwable e) {
                                log.error("Failed to check an embellished instance (" + instanceId
                                        + ") for effects caused changes to " + id + ".", e);
                            }
                        }

                        String changedMainEntityType = whelk.getStorage().getMainEntityTypeBySystemID(id);
                        if (resultingChangeObservations.size() <= MAX_OBSERVATIONS_PER_CHANGE
                                || whelk.getJsonld().isSubClassOf(changedMainEntityType, "Work")) {
                            for (Document observation : resultingChangeObservations) {
                                if (!whelk.createDocument(observation, "NotificationGenerator", "SEK", "none", false)) {
                                    log.error("Failed to create ChangeObservation:\n" + observation.getDataAsString());
                                }
                            }
                        } else {
                            if (whelk.getJsonld().isSubClassOf(changedMainEntityType, "Agent")) {
                                log.info("Changes to " + id + " would result in too many Instance-ChangeObservations, making an Agent-ChangeObservation instead.");
                                Document observation = generateObservationForChangedAgent(
                                        id, changeNotes, from.toInstant(), until.toInstant());
                                if (!whelk.createDocument(observation, "NotificationGenerator", "SEK", "none", false)) {
                                    log.error("Failed to create ChangeObservation:\n" + observation.getDataAsString());
                                }
                            } else {
                                log.info("Changes to " + id + " would result in too many ChangeObservations, skipping instead.");
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + Arrays.toString(e.getStackTrace());
            throw new RuntimeException(e);
        } finally {
            try {
                connection.close();
            } catch (java.sql.SQLException ignored) {
                // Matching the Groovy original, which did not handle close() failures here.
            }
            Map newState = new HashMap();
            newState.put("lastGenerationTime", until.toInstant().atOffset(ZoneOffset.UTC).toString());
            whelk.getStorage().putState(STATE_KEY, newState);
        }
    }

    private Document generateObservationForChangedAgent(String agentId, List<Object> changeNotes, Instant before, Instant after) {
        // No properties, we're only abusing historicEmbellish for the (identical-to-the-instance-case)-framing
        List<String> propertiesToEmbellish = List.of();
        Document agentBefore = whelk.getStorage().loadAsOf(agentId, Timestamp.from(before));
        if (agentBefore == null) { // This instance is new, and did not exist at 'before'.
            return null;
        }
        historicEmbellish(agentBefore, propertiesToEmbellish, before);
        Document agentAfter = whelk.getStorage().loadAsOf(agentId, Timestamp.from(after));
        historicEmbellish(agentAfter, propertiesToEmbellish, after);

        NotificationRules.ChangeResult comparisonResult = NotificationRules.agentRecordChanged(agentBefore, agentAfter);
        if (comparisonResult.changed()) {
            return makeChangeObservation(
                    agentId, changeNotes, "https://id.kb.se/changecategory/agent",
                    comparisonResult.before(), comparisonResult.after(), agentId);
        }

        return null;
    }

    private List<Document> generateObservationsForAffectedInstance(String instanceId, List<Object> changeNotes,
                                                                   Instant before, Instant after) {
        List<Document> generatedObservations = new ArrayList<>();

        Document instanceAfterChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(after));
        historicEmbellish(instanceAfterChange, INSTANCE_PROPERTIES_TO_EMBELLISH, after);
        Document instanceBeforeChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(before));
        if (instanceBeforeChange == null) { // This instance is new, and did not exist at 'before'.
            return generatedObservations;
        }
        historicEmbellish(instanceBeforeChange, INSTANCE_PROPERTIES_TO_EMBELLISH, before);

        // TODO? not necessarily the correct agent if multiple versions
        String agentId = null;
        if (instanceAfterChange.data.get("descriptionLastModifier") instanceof Map<?, ?> lastModifier)
            agentId = (String) lastModifier.get("@id");

        record Rule(String categoryUri, Comparison comparison) {
        }

        List<Rule> rules = List.of(
                new Rule("https://id.kb.se/changecategory/primarycontribution", NotificationRules::primaryContributionChanged),
                new Rule("https://id.kb.se/changecategory/intendedaudience", NotificationRules::intendedAudienceChanged),
                new Rule("https://id.kb.se/changecategory/agentassubject", NotificationRules::subjectChanged),
                new Rule("https://id.kb.se/changecategory/ddcclassification", NotificationRules::DDCChanged),
                new Rule("https://id.kb.se/changecategory/sabclassification", NotificationRules::SABChanged),
                new Rule("https://id.kb.se/changecategory/maintitle", NotificationRules::mainTitleChanged),
                new Rule("https://id.kb.se/changecategory/keytitle", NotificationRules::keyTitleChanged),
                new Rule("https://id.kb.se/changecategory/serialrelation", NotificationRules::serialRelationChanged),
                new Rule("https://id.kb.se/changecategory/endserial", NotificationRules::serialTerminationChanged)
        );

        for (Rule rule : rules) {
            NotificationRules.ChangeResult comparisonResult =
                    rule.comparison().compare(instanceBeforeChange, instanceAfterChange);
            if (comparisonResult.changed()) {
                generatedObservations.add(
                        makeChangeObservation(
                                instanceId, changeNotes, rule.categoryUri(),
                                comparisonResult.before(), comparisonResult.after(), agentId)
                );
            }
        }

        return generatedObservations;
    }

    @FunctionalInterface
    private interface Comparison {
        NotificationRules.ChangeResult compare(Document before, Document after);
    }

    private Document makeChangeObservation(String instanceId, List<Object> changeNotes, String categoryUri,
                                           Object oldValue, Object newValue, String agentId) {
        String newId = IdGenerator.generate();
        String metadataUri = Document.getBASE_URI().toString() + newId;
        String mainEntityUri = metadataUri + "#it";

        // If the @id is left, the object is considered a link, and the actual data (which we want) is removed when storing this as a record.
        if (oldValue instanceof Map<?, ?> oldMap && newValue instanceof Map<?, ?> newMap) {
            Map<Object, Object> oldCopy = new HashMap<>(oldMap);
            oldCopy.remove("@id");
            oldValue = oldCopy;
            Map<Object, Object> newCopy = new HashMap<>(newMap);
            newCopy.remove("@id");
            newValue = newCopy;
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("@id", metadataUri);
        record.put("@type", "Record");
        record.put("mainEntity", Map.of("@id", mainEntityUri));

        Map<String, Object> mainEntity = new LinkedHashMap<>();
        mainEntity.put("@id", mainEntityUri);
        mainEntity.put("@type", "ChangeObservation");
        mainEntity.put("concerning", Map.of("@id", Document.getBASE_URI().toString() + instanceId + "#it"));
        mainEntity.put("representationBefore", oldValue);
        mainEntity.put("representationAfter", newValue);
        mainEntity.put("category", Map.of("@id", categoryUri));
        mainEntity.put("descriptionLastModifier", singletonIdMap(agentId));

        List<String> comments = extractComments(changeNotes);
        if (!comments.isEmpty()) {
            mainEntity.put("comment", comments);
        }

        Map<String, Object> observationData = new LinkedHashMap<>();
        observationData.put("@graph", List.of(record, mainEntity));

        return new Document(observationData);
    }

    /**
     * A one-entry map, permitting a null value (unlike Map.of).
     */
    private static Map<String, Object> singletonIdMap(String id) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("@id", id);
        return map;
    }

    private List<String> extractComments(List<Object> changeNotes) {
        List<String> comments = new ArrayList<>();
        for (Object changeNote : changeNotes) {
            if (!(changeNote instanceof String changeNoteString))
                continue;
            Map changeNoteMap = null;
            try {
                changeNoteMap = mapper.readValue(changeNoteString, Map.class);
            } catch (JsonMappingException e) {
                /* ignore - this can happen when a list appears in hasChangeNote. We're not interested in those notes. */
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if (changeNoteMap != null) {
                for (Object comment : NotificationUtils.asList(changeNoteMap.get("comment")))
                    comments.add((String) comment);
            }
        }
        return comments;
    }

    /**
     * This is a simplified/specialized from of 'embellish', for historic data and using only select properties.
     * The full general embellish code can not help us here, because it is based on the idea of cached cards,
     * which can (and must!) only cache the latest/current data for each card, which isn't what we need here
     * (we need to embellish older historic data).
     *
     * This function mutates docToEmbellish
     */
    private void historicEmbellish(Document docToEmbellish, List<String> properties, Instant asOf) {
        List<Object> graphListToEmbellish = (List<Object>) docToEmbellish.data.get("@graph");
        Set<String> alreadyLoadedURIs = new LinkedHashSet<>();

        for (int i = 0; i < properties.size(); ++i) {
            Set<String> uris = findLinkedURIs(graphListToEmbellish, properties);
            uris.removeAll(alreadyLoadedURIs);
            if (uris.isEmpty())
                break;

            Map<String, Document> linkedDocumentsByUri = whelk.bulkLoad(uris, asOf);
            for (Document linkedDocument : linkedDocumentsByUri.values()) {
                List<?> linkedGraphList = (List<?>) linkedDocument.data.get("@graph");
                if (linkedGraphList.size() > 1)
                    graphListToEmbellish.add(linkedGraphList.get(1));
            }
            alreadyLoadedURIs.addAll(uris);
        }

        docToEmbellish.data = JsonLd.frame(docToEmbellish.getCompleteId(), docToEmbellish.data);
    }

    private Set<String> findLinkedURIs(Object node, List<String> properties) {
        Set<String> uris = new LinkedHashSet<>();
        if (node instanceof List<?> list) {
            for (Object element : list) {
                uris.addAll(findLinkedURIs(element, properties));
            }
        } else if (node instanceof Map<?, ?> map) {
            for (Object key : map.keySet()) {
                if (properties.contains(key)) {
                    uris.addAll(getLinkIfAny(map.get(key)));
                }
                uris.addAll(findLinkedURIs(map.get(key), properties));
            }
        }
        return uris;
    }

    private List<String> getLinkIfAny(Object node) {
        List<String> uris = new ArrayList<>();
        if (node instanceof Map<?, ?> map) {
            if (map.containsKey("@id")) {
                uris.add((String) map.get("@id"));
            }
        }
        if (node instanceof List<?> list) {
            for (Object element : list) {
                if (element instanceof Map<?, ?> map) {
                    if (map.containsKey("@id")) {
                        uris.add((String) map.get("@id"));
                    }
                }
            }
        }
        return uris;
    }
}

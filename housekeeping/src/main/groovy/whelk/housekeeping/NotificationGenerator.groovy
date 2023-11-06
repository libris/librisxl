package whelk.housekeeping

import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

import java.sql.Array
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import static whelk.util.Jackson.mapper

@CompileStatic
@Log
class NotificationGenerator extends HouseKeeper {

    public static final String STATE_KEY = "CXZ notification generator"
    private static final int MAX_OBSERVATIONS_PER_CHANGE = 100
    private String status = "OK"
    private final Whelk whelk

    public NotificationGenerator(Whelk whelk) {
        this.whelk = whelk
    }

    public String getName() {
        return "Notifications generator"
    }

    public String getStatusDescription() {
        return status
    }

    public String getCronSchedule() {
        return "* * * * *"
    }

    public void trigger() {
        // Determine the time interval of changes for which to generate notifications.
        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS)) // Default to last 24h if first time.
        Map state = whelk.getStorage().getState(STATE_KEY)
        if (state && state.lastGenerationTime)
            from = Timestamp.from( ZonedDateTime.parse( (String) state.lastGenerationTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )
        Timestamp until = Timestamp.from(Instant.now())

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Fetch all changed IDs within the interval
            String sql = "SELECT id, ARRAY_AGG(data#>>'{@graph,0,hasChangeNote}') as changeNotes FROM lddb__versions WHERE collection IN ('bib', 'auth') AND ( modified > ? AND modified <= ? ) group by id;"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            // If both an instance and one of it's dependencies are affected within the same interval, we will
            // (without this check) try to generate notifications for said instance twice.
            Set affectedInstanceIDs = []
            while (resultSet.next()) {
                String id = resultSet.getString("id")

                Array changeNotesArray = resultSet.getArray("changeNotes")

                // There is some groovy type-nonsense going on with the array types, simply doing
                // List changeNotes = Arrays.asList( changeNotesArray.getArray() ) won't work.
                List changeNotes = []
                for (Object o : changeNotesArray.getArray()) {
                    changeNotes.add(o)
                }

                generateNotificationsForChangedID(id, changeNotes, from.toInstant(),
                        until.toInstant(), affectedInstanceIDs)
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
            Map newState = new HashMap()
            newState.lastGenerationTime = until.toInstant().atOffset(ZoneOffset.UTC).toString()
            whelk.getStorage().putState(STATE_KEY, newState)
        }
    }

    /**
     * Based on the fact that 'id' has been updated, generate (if the change resulted in a ChangeNotice)
     * relevant notification-records
     */
    private void generateNotificationsForChangedID(String id, List changeNotes, Instant from, Instant until, Set affectedInstanceIDs) {

        List<Document> resultingChangeObservations = []

        List<Tuple2<String, String>> dependers = whelk.getStorage().followDependers(id, ["itemOf"])
        dependers.add(new Tuple2(id, null)) // This ID too, not _only_ the dependers!
        dependers.each {
            String dependerID =  it[0]
            String dependerMainEntityType = whelk.getStorage().getMainEntityTypeBySystemID(dependerID)
            if (whelk.getJsonld().isSubClassOf(dependerMainEntityType, "Instance")) {
                // If we've not already made an observation for this instance!
                if (!affectedInstanceIDs.contains(dependerID)) {
                    affectedInstanceIDs.add(dependerID)
                    resultingChangeObservations.addAll( generateNotificationsForAffectedInstance(dependerID, changeNotes, from, until) )
                    if (resultingChangeObservations.size() > MAX_OBSERVATIONS_PER_CHANGE) {
                        log.warn("Discarding ChangeObservations for instances related to $id, which was changed. Observations would be too many.")
                        return
                    }
                }
            }
        }

        for (Document observation : resultingChangeObservations) {
            //System.err.println(" ** Made change observation:\n${observation.getDataAsString()}")

            if (!whelk.createDocument(observation, "NotificationGenerator", "SEK", "none", false)) {
                log.error("Failed to create ChangeObservation:\n${observation.getDataAsString()}")
            }
        }
    }

    private List<Document> generateNotificationsForAffectedInstance(String instanceId, List changeNotes, Instant before, Instant after) {
        List<Document> generatedObservations = []
        List<String> propertiesToEmbellish = [
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
        ]
        Document instanceAfterChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(after))
        historicEmbellish(instanceAfterChange, propertiesToEmbellish, after)
        Document instanceBeforeChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(before))
        historicEmbellish(instanceBeforeChange, propertiesToEmbellish, before);

        // Check for primary contribution changes
        {
            Object contributionsAfter = Document._get(["mainEntity", "instanceOf", "contribution"], instanceAfterChange.data)
            Object contributionsBefore = Document._get(["mainEntity", "instanceOf", "contribution"], instanceBeforeChange.data)
            if (contributionsBefore != null && contributionsAfter != null && contributionsBefore instanceof List && contributionsAfter instanceof List) {
                for (Object contrBefore : contributionsBefore) {
                    for (Object contrAfter : contributionsAfter) {
                        if (contrBefore["@type"].equals("PrimaryContribution") && contrAfter["@type"].equals("PrimaryContribution")) {
                            if (contributionsBefore["agent"] != null && contributionsAfter["agent"] != null) {
                                if (
                                        contributionsBefore["agent"]["familyName"] != contributionsAfter["agent"]["familyName"] ||
                                                contributionsBefore["agent"]["givenName"] != contributionsAfter["agent"]["givenName"] ||
                                                contributionsBefore["agent"]["lifeSpan"] != contributionsAfter["agent"]["lifeSpan"]
                                )
                                    generatedObservations.add(
                                            makeChangeObservation(instanceId, changeNotes,
                                                    "https://id.kb.se/changecategory/primarycontribution",
                                                    (Map) contrBefore, (Map) contrAfter))
                            }
                        }
                    }
                }
            }
        }

        return generatedObservations
    }

    private Document makeChangeObservation(String instanceId, List changeNotes, String categoryUri, Map oldValue, Map newValue) {
        String newId = IdGenerator.generate()
        String metadataUri = Document.BASE_URI.toString() + newId
        String mainEntityUri = metadataUri+"#it"

        Map observationData = [ "@graph":[
                [
                        "@id" : metadataUri,
                        "@type" : "Record",
                        "mainEntity" : ["@id" : mainEntityUri],
                ],
                [
                        "@id" : mainEntityUri,
                        "@type" : "ChangeObservation",
                        "about" : ["@id" : Document.BASE_URI.toString() + instanceId],
                        "representationBefore" : oldValue,
                        "representationAfter" : newValue,
                        "category" : ["@id" : categoryUri],
                ]
        ]]

        List<String> comments = extractComments(changeNotes)
        if (comments) {
            observationData["@graph"][1]["comment"] = comments
        }

        return new Document(observationData)
    }

    private List<String> extractComments(List changeNotes) {
        List<String> comments = []
        for (Object changeNote : changeNotes) {
            if ( ! (changeNote instanceof String) )
                continue
            Map changeNoteMap = mapper.readValue( (String) changeNote, Map)
            comments.addAll( asList(changeNoteMap["comment"]) )
        }
        return comments
    }

    private List asList(Object o) {
        if (o == null)
            return []
        if (o instanceof List)
            return o
        return [o]
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
        List graphListToEmbellish = (List) docToEmbellish.data["@graph"]
        Set alreadyLoadedURIs = []

        for (int i = 0; i < properties.size(); ++i) {
            Set uris = findLinkedURIs(graphListToEmbellish, properties)
            uris.removeAll(alreadyLoadedURIs)
            if (uris.isEmpty())
                break

            Map<String, Document> linkedDocumentsByUri = whelk.bulkLoad(uris, asOf)
            linkedDocumentsByUri.each {
                List linkedGraphList = (List) it.value.data["@graph"]
                if (linkedGraphList.size() > 1)
                    graphListToEmbellish.add(linkedGraphList[1])
            }
            alreadyLoadedURIs.addAll(uris)
        }

        docToEmbellish.data = JsonLd.frame(docToEmbellish.getCompleteId(), docToEmbellish.data)
    }

    private Set<String> findLinkedURIs(Object node, List<String> properties) {
        Set<String> uris = []
        if (node instanceof List) {
            for (Object element : node) {
                uris.addAll(findLinkedURIs(element, properties))
            }
        }
        else if (node instanceof Map) {
            for (String key : node.keySet()) {
                if (properties.contains(key)) {
                    uris.addAll(getLinkIfAny(node[key]))
                }
                uris.addAll(findLinkedURIs(node[key], properties))
            }
        }
        return uris
    }

    private List<String> getLinkIfAny(Object node) {
        List<String> uris = []
        if (node instanceof Map) {
            if (node.containsKey("@id")) {
                uris.add((String) node["@id"])
            }
        }
        if (node instanceof List) {
            for (Object element : node) {
                if (element instanceof Map) {
                    if (element.containsKey("@id")) {
                        uris.add((String) element["@id"])
                    }
                }
            }
        }
        return uris
    }

}

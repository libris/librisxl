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

import static whelk.util.Jackson.mapper

@CompileStatic
@Log
class NotificationGenerator extends HouseKeeper {

    public static final String STATE_KEY = "CXZ notification generator"
    private static final int MAX_OBSERVATIONS_PER_CHANGE = 20
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
        Instant now = Instant.now()
        Timestamp from = Timestamp.from(now) // First run? Default to now (=do nothing but set the timestamp for next run)
        Map state = whelk.getStorage().getState(STATE_KEY)
        if (state && state.lastGenerationTime)
            from = Timestamp.from( ZonedDateTime.parse( (String) state.lastGenerationTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )
        Timestamp until = Timestamp.from(now)

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        Map<String, List<String>> changedInstanceIDsWithComments = [:]

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Fetch all changed IDs within the interval
            String sql = "SELECT id, ARRAY_AGG(data#>>'{@graph,0,hasChangeNote}') as changeNotes FROM lddb__versions WHERE collection IN ('bib', 'auth') AND deleted = false AND ( modified > ? AND modified <= ? ) group by id;"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            while (resultSet.next()) {
                String id = resultSet.getString("id")

                Array changeNotesArray = resultSet.getArray("changeNotes")
                List changeNotes = []
                for (Object o : changeNotesArray.getArray()) {
                    if (o != null)
                        changeNotes.add(o)
                }

                List<Tuple2<String, String>> dependers = whelk.getStorage().followDependers(id, ["itemOf"])
                dependers.add(new Tuple2(id, null)) // This ID too, not _only_ the dependers!
                dependers.each {
                    String dependerID =  it[0]
                    String dependerMainEntityType = whelk.getStorage().getMainEntityTypeBySystemID(dependerID)
                    if (whelk.getJsonld().isSubClassOf(dependerMainEntityType, "Instance")) {
                        if (!changedInstanceIDsWithComments.containsKey(dependerID)) {
                            changedInstanceIDsWithComments.put(dependerID, [])
                        }
                        changedInstanceIDsWithComments[dependerID].addAll(changeNotes)
                    }
                }
            }

            for (String instanceId : changedInstanceIDsWithComments.keySet()) {
                List<Document> resultingChangeObservations = generateObservationsForAffectedInstance(
                                instanceId, changedInstanceIDsWithComments[instanceId], from.toInstant(), until.toInstant())

                if (resultingChangeObservations.size() <= MAX_OBSERVATIONS_PER_CHANGE) {
                    for (Document observation : resultingChangeObservations) {
                        if (!whelk.createDocument(observation, "NotificationGenerator", "SEK", "none", false)) {
                            log.error("Failed to create ChangeObservation:\n${observation.getDataAsString()}")
                        }
                    }
                }
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

    private List<Document> generateObservationsForAffectedInstance(String instanceId, List changeNotes, Instant before, Instant after) {
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
                "isPartOf",
                "subject",
                "continuedBy",
                "continues",
        ]
        Document instanceAfterChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(after))
        historicEmbellish(instanceAfterChange, propertiesToEmbellish, after)
        Document instanceBeforeChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(before))
        if (instanceBeforeChange == null) { // This instance is new, and did not exist at 'before'.
            return generatedObservations
        }
        historicEmbellish(instanceBeforeChange, propertiesToEmbellish, before)
        String agentId = instanceAfterChange.data?['descriptionLastModifier']?['@id'] // TODO? not necessarily the correct agent if multiple versions
        Tuple comparisonResult

        // Primary Contribution
        comparisonResult = NotificationRules.primaryContributionChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/primarycontribution",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Intended Audience
        comparisonResult = NotificationRules.intendedAudienceChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/intendedaudience",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Subject (agent)
        comparisonResult = NotificationRules.subjectChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/agentassubject",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // DDC classification
        comparisonResult = NotificationRules.DDCChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/ddcclassification",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // SAB classification
        comparisonResult = NotificationRules.SABChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/sabclassification",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Main Title
        comparisonResult = NotificationRules.mainTitleChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/maintitle",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Key Title
        comparisonResult = NotificationRules.keyTitleChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/keytitle",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Serial relation
        comparisonResult = NotificationRules.serialRelationChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/serialrelation",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        // Serial termination
        comparisonResult = NotificationRules.serialTerminationChanged(instanceBeforeChange, instanceAfterChange)
        if (comparisonResult[0]) {
            generatedObservations.add(
                    makeChangeObservation(
                            instanceId, changeNotes, "https://id.kb.se/changecategory/endserial",
                            comparisonResult[1], comparisonResult[2], agentId)
            )
        }

        return generatedObservations
    }

    private Document makeChangeObservation(String instanceId, List changeNotes, String categoryUri, Object oldValue, Object newValue, String agentId) {
        String newId = IdGenerator.generate()
        String metadataUri = Document.BASE_URI.toString() + newId
        String mainEntityUri = metadataUri+"#it"

        // If the @id is left, the object is considered a link, and the actual data (which we want) is removed when storing this as a record.
        if (oldValue instanceof Map && newValue instanceof Map) {
            oldValue = new HashMap(oldValue)
            oldValue.remove("@id")
            newValue = new HashMap(newValue)
            newValue.remove("@id")
        }

        Map observationData = [ "@graph":[
                [
                        "@id" : metadataUri,
                        "@type" : "Record",
                        "mainEntity" : ["@id" : mainEntityUri],
                ],
                [
                        "@id" : mainEntityUri,
                        "@type" : "ChangeObservation",
                        "concerning" : ["@id" : Document.BASE_URI.toString() + instanceId + '#it'],
                        "representationBefore" : oldValue,
                        "representationAfter" : newValue,
                        "category" : ["@id" : categoryUri],
                        "agent" : ["@id" : agentId],
                ]
        ]]

        List<String> comments = extractComments(changeNotes)
        if (comments) {
            Map mainEntity = (Map) observationData["@graph"][1]
            mainEntity.put("comment", comments)
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

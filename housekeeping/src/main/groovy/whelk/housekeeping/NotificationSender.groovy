package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import whelk.Document
import whelk.JsonLd
import whelk.Whelk

import java.sql.*
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import static whelk.util.Jackson.mapper

@CompileStatic
@Log4j2
class NotificationSender extends HouseKeeper {

    private final String STATE_KEY = "CXZ notification email sender"
    private String status = "OK"
    private final Whelk whelk

    public NotificationSender(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    String getName() {
        return "Notifications sender"
    }

    @Override
    String getStatusDescription() {
        return status
    }

    public String getCronSchedule() {
        return "0 6 * * *"
    }

    @Override
    void trigger() {
        Map<String, List<Map>> heldByToUserSettings = NotificationUtils.getAllSubscribingUsers(whelk)

        // Determine the time interval of ChangeObservations to consider
        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS)) // Default to last 24h if first time.
        Map sendState = whelk.getStorage().getState(STATE_KEY)
        if (sendState && sendState.notifiedChangesUpTo)
            from = Timestamp.from( ZonedDateTime.parse( (String) sendState.notifiedChangesUpTo, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        Instant notifiedChangesUpTo = from.toInstant()

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            String sql = "SELECT MAX(created) as lastChange, data#>>'{@graph,1,concerning,@id}' as concerningUri, ARRAY_AGG(data::text) as data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND created > ? GROUP BY data#>>'{@graph,1,concerning,@id}';"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            //System.err.println("  **  Searching for Observations: " + statement)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()

            while (resultSet.next()) {
                String concerningUri = resultSet.getString("concerningUri")
                Array changeObservationsArray = resultSet.getArray("data")
                // Groovy..
                List changeObservationsForConcerned = []
                for (Object o : changeObservationsArray.getArray()) {
                    changeObservationsForConcerned.add(o)
                }

                sendFor(concerningUri, heldByToUserSettings, changeObservationsForConcerned)

                Instant lastChangeObservationForInstance = resultSet.getTimestamp("lastChange").toInstant()
                if (lastChangeObservationForInstance.isAfter(notifiedChangesUpTo))
                    notifiedChangesUpTo = lastChangeObservationForInstance
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
            if (notifiedChangesUpTo.isAfter(from.toInstant())) {
                Map newState = new HashMap()
                newState.notifiedChangesUpTo = notifiedChangesUpTo.atOffset(ZoneOffset.UTC).toString()
                whelk.getStorage().putState(STATE_KEY, newState)
            }
        }

    }

    private void sendFor(String concerningUri, Map<String, List<Map>> heldByToUserSettings, List changeObservationsForConcerned) {
        String concerningSystemId = whelk.getStorage().getSystemIdByIri(concerningUri)
        String type = whelk.getStorage().getMainEntityTypeBySystemID(concerningSystemId)

        if (whelk.getJsonld().isSubClassOf(type, "Instance"))
            sendForInstance(concerningSystemId, heldByToUserSettings, changeObservationsForConcerned)
        else if (whelk.getJsonld().isSubClassOf(type, "Agent"))
            sendForAgent(concerningSystemId, heldByToUserSettings, changeObservationsForConcerned)
    }

    private void sendForAgent(String concerningId, Map<String, List<Map>> heldByToUserSettings, List changeObservationsForConcerned) {
        List<String> concernedLibraries = whelk.getStorage().followLibrariesConcernedWith(concerningId)
        String subject = NotificationUtils.subject(whelk, NotificationUtils.NotificationType.ChangeObservation, concernedLibraries)

        List<Map> changeObservationMaps = []
        for (String observationDataString : changeObservationsForConcerned) {
            Map changeObservationMap = mapper.readValue( (String) observationDataString, Map )
            if (changeObservationMap != null)
                changeObservationMaps.add(changeObservationMap)
        }

        Set alreadySentTo = [] as Set

        for (String library : concernedLibraries) {
            List<Map> users = (List<Map>) heldByToUserSettings[library]
            if (users) {
                for (Map user : users) {
                    if ( user?.notificationCategories?.find { it["@id"] == "https://id.kb.se/changecategory/agent" } != null ) {

                        if (!changeObservationMaps.isEmpty() && user.notificationEmail &&
                                user.notificationEmail instanceof String && !alreadySentTo.contains(user.notificationEmail)) {
                            String body = generateEmailBody(concerningId, changeObservationMaps as Set)
                            NotificationUtils.sendEmail((String) user.notificationEmail, subject, body)
                            alreadySentTo.add(user.notificationEmail)
                        }

                    }
                }
            }
        }

    }

    private void sendForInstance(String instanceId, Map<String, List<Map>> heldByToUserSettings, List changeObservationsForInstance) {
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(instanceId)
        String subject = NotificationUtils.subject(whelk, NotificationUtils.NotificationType.ChangeObservation, libraries)

        for (String library : libraries) {
            List<Map> users = (List<Map>) heldByToUserSettings[library]
            if (users) {
                for (Map user : users) {
                    /* 'user' is now a map looking something like this:
                    {
                        "notificationEmail": "...",
                        "notificationCategories": [
                            {
                                "@id": "https://id.kb.se/changecategory/maintitle"
                            },
                            ...
                        ],
                        "notificationCollections": [
                            {
                                "@id": "https://libris.kb.se/library/Utb1"
                            },
                            ...
                        ]
                    }
                    */

                    Set<Map> matchedObservations = new LinkedHashSet<>()

                    user?.notificationCategories?.each { Map request ->
                        String trigger = request?["@id"]
                        if (trigger != null) {
                            Map triggeredObservation = matches(trigger, changeObservationsForInstance)
                            if (triggeredObservation != null) {
                                matchedObservations.add(triggeredObservation)
                            }
                        }
                    }

                    if (!matchedObservations.isEmpty() && user.notificationEmail && user.notificationEmail instanceof String) {
                        String body = generateEmailBody(instanceId, matchedObservations)
                        NotificationUtils.sendEmail((String) user.notificationEmail, subject, body)
                    }
                }
            }
        }

    }

    private Map matches(String trigger, List changeObservationsForInstance) {
        for (Object obj : changeObservationsForInstance) {
            Map changeObservationMap = mapper.readValue( (String) obj, Map )
            List graphList = changeObservationMap["@graph"]
            Map mainEntity = graphList?[1]
            String category = mainEntity?.category["@id"]
            if (category && category == trigger)
                return changeObservationMap
        }
        return null
    }

    private String generateEmailBody(String changedInstanceId, Set<Map> triggeredObservations) {
        Document current = whelk.getStorage().load(changedInstanceId)
        StringBuilder sb = new StringBuilder()
        sb.append(NotificationUtils.describe(current, whelk)).append('\n')
        sb.append(current.getControlNumber()).append("\n")
        sb.append(NotificationUtils.makeLink(changedInstanceId)).append("\n")
        sb.append("\n")

        boolean commentsRendered = false
        for (Map observation : triggeredObservations) {
            String observationUri = Document._get(["@graph", 1, "@id"], observation)
            if (!observationUri)
                continue

            if (!commentsRendered) {
                commentsRendered = true
                Object comments = Document._get(["@graph", 1, "comment"], observation)

                if (comments instanceof List) {
                    sb.append("\n\tÄndringsanmärkningar\n")
                    for (String comment : comments)
                        sb.append('\t\t- ' + comment.replace('\n', '\n\t\t') + "\n")
                }
                sb.append("\n")
            }

            String observationId = whelk.getStorage().getSystemIdByIri(observationUri)
            Document embellishedObservation = whelk.loadEmbellished(observationId)
            Map framed = JsonLd.frame(observationUri, embellishedObservation.data)

            Map category = whelk.getJsonld().applyLensAsMapByLang( (Map) framed["category"], ["sv"] as Set, [], ["chips"])
            sb.append("\t" + category["sv"])

            if (framed["representationBefore"] instanceof Map && framed["representationAfter"] instanceof Map) {
                Map before = whelk.getJsonld().applyLensAsMapByLang((Map) framed["representationBefore"], ["sv"] as Set, [], ["chips"])
                Map after = whelk.getJsonld().applyLensAsMapByLang((Map) framed["representationAfter"], ["sv"] as Set, [], ["chips"])
                sb.append("\n\t\tInnan: " + before["sv"])
                sb.append("\n\t\tEfter: " + after["sv"])
            } else if (framed["representationBefore"] instanceof List && framed["representationAfter"] instanceof List) {
                sb.append("\n\t\tInnan: ")
                for (Object item : framed["representationBefore"]) {
                    Map before = whelk.getJsonld().applyLensAsMapByLang((Map) item, ["sv"] as Set, [], ["chips"])
                    sb.append((String) before["sv"] + ", ")
                }
                sb.append("\n\t\tEfter: ")
                for (Object item : framed["representationAfter"]) {
                    Map after = whelk.getJsonld().applyLensAsMapByLang((Map) item, ["sv"] as Set, [], ["chips"])
                    sb.append((String) after["sv"] + ", ")
                }
            }

            sb.append("\n\n")
        }

        return sb.toString()
    }
}

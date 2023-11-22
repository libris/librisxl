package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import whelk.Whelk

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
@Log4j2
class InquirySender extends HouseKeeper {
    private final String STATE_KEY = "CXZ inquiry email sender"
    private String status = "OK"
    private final Whelk whelk

    public InquirySender(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    String getName() {
        return "Inquiry sender"
    }

    @Override
    String getStatusDescription() {
        return status
    }

    public String getCronSchedule() {
        return "* * * * *"
    }

    @Override
    void trigger() {

        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS))
        Map sendState = whelk.getStorage().getState(STATE_KEY)
        if (sendState && sendState.notifiedChangesUpTo)
            from = Timestamp.from( ZonedDateTime.parse( (String) sendState.notifiedChangesUpTo, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        Instant notifiedChangesUpTo = from.toInstant()

        Map<String, List<Map>> heldByToUserSettings = NotificationUtils.getAllSubscribingUsers(whelk)

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            String sql = "SELECT modified, data#>>'{@graph,1}' as data FROM lddb WHERE deleted = false AND data#>>'{@graph,1,@type}' IN ('InquiryAction', 'ChangeNotice') AND modified > ?;"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()

            while (resultSet.next()) {
                String dataString = resultSet.getString("data")
                Map data = mapper.readValue( dataString, Map )

                /* Assume data:
                    {
                        "@id": "http://libris.kb.se.localhost:5000/xflpmzvsv9nfr5q0#it",
                        "@type": "InquiryAction",
                        "comment": [
                            "Det h\u00e4r \u00e4r en fr\u00e5ga!"
                        ],
                        "concerning": [
                            {
                                "@id": "http://libris.kb.se.localhost:5000/s93qhl340tcvtcp#it"
                            }
                        ]
                    }
                 */

                // Compile list of concerned records
                List<String> concerningSystemIDs = []
                if (data["concerning"]) {
                    NotificationUtils.asList(data["concerning"]).each { link ->
                        if (link != null && link instanceof Map && link["@id"] != null) {
                            String uri = link["@id"]
                            String instanceId = whelk.getStorage().getSystemIdByIri((String) uri)
                            if (instanceId != null)
                                concerningSystemIDs.add(instanceId)
                        }
                    }
                }

                // Figure out who to send to
                Set<String> recipients = []
                boolean sendToAll = true
                concerningSystemIDs.each { String concerningSystemID ->
                    String type = whelk.getStorage().getMainEntityTypeBySystemID(concerningSystemID)
                    if (whelk.getJsonld().isSubClassOf(type, "Instance")) {
                        sendToAll = false
                        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(concerningSystemID)
                        for (String library : libraries) {
                            List<Map> usersSubbedToLibrary = heldByToUserSettings[library] ?: []
                            for (Map user : usersSubbedToLibrary) {
                                Object email = user["notificationEmail"]
                                if (email != null && email instanceof String) {
                                    recipients.add(email)
                                }
                            }
                        }
                    }
                }
                if (sendToAll) {
                    heldByToUserSettings.keySet().each { String library ->
                        List<Map> usersSubbedToLibrary = heldByToUserSettings[library]
                        for (Map user : usersSubbedToLibrary) {
                            Object email = user["notificationEmail"]
                            if (email != null && email instanceof String) {
                                recipients.add(email)
                            }
                        }
                    }
                }

                String body = generateEmailBody(concerningSystemIDs, NotificationUtils.asList(data["comment"]))

                for (String recipient : recipients) {
                    NotificationUtils.sendEmail(recipient, "CXZ", body)
                }

                Instant lastChangeObservationForInstance = resultSet.getTimestamp("modified").toInstant()
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

    private String generateEmailBody(List<String> concerningSystemIDs, List<String> comments) {
        return "concerning: "+concerningSystemIDs + "\ncomments: "+comments
    }
}

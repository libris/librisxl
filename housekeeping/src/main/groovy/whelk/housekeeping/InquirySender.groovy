package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.util.DocumentUtil

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
                var messageType = NotificationUtils.NotificationType.valueOf((String) data['@type'])

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
                String subject = NotificationUtils.subject(whelk, messageType)
                for (String concerningSystemID : concerningSystemIDs) {
                    String type = whelk.getStorage().getMainEntityTypeBySystemID(concerningSystemID)
                    if (whelk.getJsonld().isSubClassOf(type, "Instance")) { // Send to all holders of said instance
                        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(concerningSystemID)
                        recipients.addAll( getRecipientsForLibraries(libraries, heldByToUserSettings) )

                        subject = NotificationUtils.subject(whelk, messageType, libraries)
                    } else if (whelk.getJsonld().isSubClassOf(type, "Work")) { // Send to all holders of non-electronic instances of said work
                        List<String> instances = getNonElectronicInstancesOf(concerningSystemID)
                        for (String instanceID : instances) {
                            List<String> libraries = whelk.getStorage().getAllLibrariesHolding(instanceID)
                            recipients.addAll( getRecipientsForLibraries(libraries, heldByToUserSettings) )
                        }
                    }
                    else { // Usually an Agent, but anything goes. Send to all holders of something affected
                        List<String> libraries = whelk.getStorage().followLibrariesConcernedWith(concerningSystemID)
                        recipients.addAll( getRecipientsForLibraries(libraries, heldByToUserSettings) )
                    }
                }

                // Send
                String noticeSystemId = whelk.getStorage().getSystemIdByIri((String) data["@id"])
                var creatorId = Optional.ofNullable(DocumentUtil.getAtPath(data, ['descriptionCreator', JsonLd.ID_KEY]) as String)
                String body = generateEmailBody(
                        messageType,
                        noticeSystemId,
                        concerningSystemIDs,
                        NotificationUtils.asList(data["comment"]),
                        creatorId)
                log.info("Sending ${recipients.size()} emails for $noticeSystemId")
                for (String recipient : recipients) {
                    NotificationUtils.sendEmail(recipient, subject, body)
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

    private List<String> getRecipientsForLibraries(List<String> libraries, Map<String, List<Map>> heldByToUserSettings) {
        List<String> recipients = []
        for (String library : libraries) {
            List<Map> usersSubbedToLibrary = heldByToUserSettings[library] ?: []
            for (Map user : usersSubbedToLibrary) {
                Object email = user["notificationEmail"]
                if (email != null && email instanceof String) {
                    recipients.add(email)
                }
            }
        }
        return recipients
    }

    private List<String> getNonElectronicInstancesOf(String workId) {
        List<String> result = []
        String sql = """
            SELECT
              l.id, l.data#>>'{@graph,1,@type}'
            FROM
              lddb__dependencies d
            LEFT JOIN
              lddb l on d.id = l.id
            WHERE
              l.deleted = false AND
              d.dependsonid = ? AND
              d.relation = 'instanceOf' AND
              l.data#>>'{@graph,1,@type}' <> 'Electronic'
            """.stripIndent()
        whelk.getStorage().withDbConnection({
            PreparedStatement statement = whelk.getStorage().getMyConnection().prepareStatement(sql)
            statement.setString(1, workId)
            ResultSet resultSet = statement.executeQuery()
            while (resultSet.next()) {
                result.add( resultSet.getString("id") )
            }
        })
        return result
    }

    private String generateEmailBody(NotificationUtils.NotificationType messageType, String noticeSystemId, List<String> concerningSystemIDs, List<String> comments, Optional<String> creatorId) {
        StringBuilder sb = new StringBuilder()

        if (comments.size() < 2) {
            for (String comment : comments) {
                sb.append("- ").append(comment).append("\n")
            }
        } else {
            sb.append("Senaste:\n")
            sb.append("- ").append(comments.last()).append("\n")
            sb.append("\n")
            sb.append("Alla:\n")
            for (String comment : comments) {
                sb.append("- ").append(comment).append("\n")
            }
        }
        sb.append("\n")

        if (messageType == NotificationUtils.NotificationType.InquiryAction) {
            sb.append("Länk till förfrågan:\n")
        } else if (messageType == NotificationUtils.NotificationType.ChangeNotice) {
            sb.append("Länk till meddelande:\n")
        }
        sb.append( NotificationUtils.makeLink(noticeSystemId) ).append('\n')

        sb.append('\n')
        creatorId.ifPresent {
            var creatorLabel = whelk.jsonld.vocabIndex['descriptionCreator']?['labelByLang']?['sv'] ?: ""
            var creator = NotificationUtils.chipString(DocumentUtil.getAtPath(whelk.loadData(it), [JsonLd.GRAPH_KEY, 1]), whelk)
            sb.append(creatorLabel).append(': ').append(creator).append('\n')
            sb.append('\n')
        }
        if (concerningSystemIDs) {
            sb.append(NotificationUtils.DIVIDER).append('\n')
            sb.append("Gäller:").append('\n')
            sb.append('\n')
            for (String systemId : concerningSystemIDs) {
                Document doc = whelk.loadEmbellished(systemId)
                sb.append(NotificationUtils.describe(doc, whelk)).append('\n')
                sb.append(NotificationUtils.makeLink(systemId)).append("\n")
                sb.append("\n")
                sb.append("\n")
            }
        }
        sb.append(NotificationUtils.EMAIL_FOOTER)

        return sb.toString()
    }
}

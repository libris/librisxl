package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import org.simplejavamail.api.email.Email
import org.simplejavamail.api.mailer.Mailer
import org.simplejavamail.email.EmailBuilder
import org.simplejavamail.mailer.MailerBuilder
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.util.PropertyLoader

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
@Log4j2
class NotificationSender extends HouseKeeper {

    private final String STATE_KEY = "CXZ notification email sender"
    private String status = "OK"
    private final Whelk whelk
    private final Mailer mailer
    private final String senderAddress

    public NotificationSender(Whelk whelk) {
        this.whelk = whelk
        Properties props = PropertyLoader.loadProperties("secret")
        if (props.containsKey("smtpServer") &&
                props.containsKey("smtpPort") &&
                props.containsKey("smtpSender") &&
                props.containsKey("smtpUser") &&
                props.containsKey("smtpPassword"))
            mailer = MailerBuilder
                    .withSMTPServer(
                            (String) props.get("smtpServer"),
                            Integer.parseInt((String)props.get("smtpPort")),
                            (String) props.get("smtpUser"),
                            (String) props.get("smtpPassword")
                    ).buildMailer()
        senderAddress = props.get("smtpSender")
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
        return "* * * * *"
    }

    @Override
    void trigger() {
        // Build a multi-map of library -> list of settings objects for that library's users
        Map<String, List<Map>> heldByToUserSettings = new HashMap<>();
        {
            List<Map> allUserSettingStrings = whelk.getStorage().getAllUserData()
            for (Map settings : allUserSettingStrings) {
                if (!settings["notificationEmail"])
                    continue
                settings?.requestedNotifications?.each { request ->
                    if (!request instanceof Map)
                        return
                    if (!request["heldBy"])
                        return

                    String heldBy = request["heldBy"]
                    if (!heldByToUserSettings.containsKey(heldBy))
                        heldByToUserSettings.put(heldBy, [])
                    heldByToUserSettings[heldBy].add(settings)
                }
            }
        }

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
            String sql = "SELECT MAX(created) as lastChange, data#>>'{@graph,1,about,@id}' as instanceUri, ARRAY_AGG(data::text) as data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND created > ? GROUP BY data#>>'{@graph,1,about,@id}';"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            //System.err.println("  **  Searching for Observations: " + statement)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()

            while (resultSet.next()) {
                String instanceUri = resultSet.getString("instanceUri")
                Array changeObservationsArray = resultSet.getArray("data")
                // Groovy..
                List changeObservationsForInstance = []
                for (Object o : changeObservationsArray.getArray()) {
                    changeObservationsForInstance.add(o)
                }

                sendFor(instanceUri, heldByToUserSettings, changeObservationsForInstance)

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

    private void sendFor(String instanceUri, Map<String, List<Map>> heldByToUserSettings, List changeObservationsForInstance) {
        String instanceId = whelk.getStorage().getSystemIdByIri(instanceUri)
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(instanceId)

        for (String library : libraries) {
            List<Map> users = (List<Map>) heldByToUserSettings[library]
            if (users) {
                for (Map user : users) {
                    /*
                    'user' is now a map looking something like this:
                    {
                        "id": "sldknfslkdnsdlkgnsdkjgnb"
                        "requestedNotifications": [
                            {
                                "heldBy": "https://libris.kb.se/library/Utb1",
                                "triggers": [
                                    "https://id.kb.se/changecategory/primarycontribution"
                                ]
                            }
                        ],
                        "notificationEmail": "noreply@kb.se"
                    }*/

                    List<Map> matchedObservations = []

                    user?.requestedNotifications?.each { Map request ->
                        request?.triggers?.each { String trigger ->
                            Map triggeredObservation = matches(trigger, changeObservationsForInstance)
                            if (triggeredObservation != null) {
                                matchedObservations.add(triggeredObservation)
                            }
                        }
                    }

                    if (!matchedObservations.isEmpty() && user.notificationEmail && user.notificationEmail instanceof String) {
                        String body = generateEmailBody(instanceId, matchedObservations)
                        sendEmail(senderAddress, (String) user.notificationEmail, "CXZ", body)

                        System.err.println("Now send email to " + user.notificationEmail + "\n\t" + body)
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

    private void sendEmail(String sender, String recipient, String subject, String body) {
        if (mailer) {
            Email email  = EmailBuilder.startingBlank()
                    .to(recipient)
                    .withSubject(subject)
                    .from(sender)
                    .withPlainText(body)
                    .buildEmail()

            log.info("Sending notification (cxz) email to " + recipient)
            mailer.sendMail(email)
        } else {
            log.info("Should now have sent notification (cxz) email to " + recipient + " but SMTP is not configured.")
        }
    }

    private String generateEmailBody(String changedInstanceId, List<Map> triggeredObservations) {

        Document current = whelk.getStorage().load(changedInstanceId)
        String mainTitle = Document._get(["@graph", 1, "hasTitle", 0, "mainTitle"], current.data)

        StringBuilder sb = new StringBuilder()
        sb.append("Ändringar har skett i instans: " + Document.BASE_URI.resolve(changedInstanceId).toString())
        if (mainTitle)
            sb.append(" (" + mainTitle + ")\n")
        else
            sb.append("\n")

        for (Map observation : triggeredObservations) {
            String observationUri = Document._get(["@graph", 1, "@id"], observation)
            if (!observationUri)
                continue

            String observationId = whelk.getStorage().getSystemIdByIri(observationUri)
            Document embellishedObservation = whelk.loadEmbellished(observationId)
            Map framed = JsonLd.frame(observationUri, embellishedObservation.data)

            Map category = whelk.getJsonld().applyLensAsMapByLang( (Map) framed["category"], ["sv"] as Set, [], ["chips"])
            Map before = whelk.getJsonld().applyLensAsMapByLang( (Map) framed["representationBefore"], ["sv"] as Set, [], ["chips"])
            Map after = whelk.getJsonld().applyLensAsMapByLang( (Map) framed["representationAfter"], ["sv"] as Set, [], ["chips"])
            sb.append("\tÄndring avser kategorin: "+ category["sv"])
            sb.append("\n\t\tInnan ändring: " + before["sv"])
            sb.append("\n\t\tEfter ändring: " + after["sv"])

            if (observation["comment"]) {
                sb.append("\n\t\tTillhörande kommentarer:")
                for (String comment : observation["comment"])
                    sb.append("\n\t\t\t"+comment)
            }
            sb.append("\n\n")
        }

        return sb.toString()
    }
}

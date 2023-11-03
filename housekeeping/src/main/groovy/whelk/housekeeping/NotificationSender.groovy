package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import org.simplejavamail.api.email.Email
import org.simplejavamail.api.mailer.Mailer
import org.simplejavamail.email.EmailBuilder
import org.simplejavamail.mailer.MailerBuilder
import whelk.Document
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
        if (sendState && sendState.lastEmailTime)
            from = Timestamp.from( ZonedDateTime.parse( (String) sendState.lastEmailTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )

        Timestamp until = null
        Map generationState = whelk.getStorage().getState(NotificationGenerator.STATE_KEY)
        if (generationState && generationState.lastGenerationTime)
            until = Timestamp.from( ZonedDateTime.parse( (String) generationState.lastGenerationTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )
        if (until == (Object) null) // Groovy...
            return
        if (until.toInstant().isBefore(from.toInstant()))
            return


        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Fetch all changed IDs within the interval
            //String sql = "SELECT id, data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND ( created > ? AND created <= ? );"
            String sql = "SELECT data#>>'{@graph,1,about,@id}' as instanceUri, ARRAY_AGG(data::text) as data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND ( created > ? AND created <= ? ) GROUP BY data#>>'{@graph,1,about,@id}';"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            System.err.println("  **  Searching for Observations: " + statement)
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

                System.err.println("About to email for instance: " + instanceUri)

                sendFor(instanceUri, heldByToUserSettings, changeObservationsForInstance)
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
            Map newState = new HashMap()
            newState.lastEmailTime = until.toInstant().atOffset(ZoneOffset.UTC).toString()
            whelk.getStorage().putState(STATE_KEY, newState)
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

                    List<String> triggeredCategories = []

                    user?.requestedNotifications?.each { Map request ->
                        request?.triggers?.each { String trigger ->
                            if (matches(trigger, changeObservationsForInstance)) {
                                triggeredCategories.add(trigger)
                            }
                        }
                    }

                    if (!triggeredCategories.isEmpty() && user.notificationEmail && user.notificationEmail instanceof String) {
                        String body = generateEmailBody(instanceId, triggeredCategories)
                        sendEmail(senderAddress, (String) user.notificationEmail, "CXZ", body)

                        System.err.println("Now send email to " + user.notificationEmail + "\n\t" + body)
                    }
                }
            }
        }

    }

    private boolean matches(String trigger, List changeObservationsForInstance) {
        for (Object obj : changeObservationsForInstance) {
            Map changeObservationMap = mapper.readValue( (String) obj, Map )
            List graphList = changeObservationMap["@graph"]
            Map mainEntity = graphList?[1]
            String category = mainEntity?.category
            if (category && category == trigger)
                return true
        }
        return false
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
            log.info("Should now have sent notification (cxz) email to " + recipient + " but SMTP not configured.")
        }
    }

    private String generateEmailBody(String changedInstanceId, List<String> triggeredCategories) {
        return "Ändring av " + changedInstanceId + " kategorier: " + triggeredCategories
    }
}

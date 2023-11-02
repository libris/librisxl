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
        if (!until)
            return


        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Fetch all changed IDs within the interval
            String sql = "SELECT id, data FROM lddb WHERE data#>>'{@graph,1,@type}' = 'ChangeObservation' AND ( created > ? AND created <= ? );"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            System.err.println("  **  Searching for Observations: " + statement)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()

            while (resultSet.next()) {
                String id = resultSet.getString("id")
                System.err.println("  ******  ChangeObservation: " + id + " picked up for emailing!")
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

    /**
     * Generate notifications for an affected bibliographic instance. Beware: fromVersion and untilVersion may not be
     * _of this document_ (id), but rather of a document this instance depends on!
     */

    private void generateNotificationsForAffectedInstance(String id, Map heldByToUserSettings, Instant from, Instant until) {
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(id)
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
                                    "https://id.kb.se/changenote/primarytitle"
                                ]
                            }
                        ],
                        "email": "noreply@kb.se"
                    }*/

                    List<String> triggered = changeMatchesAnyTrigger(
                            id, from,
                            until, user, library)
                    if (triggered) {

                        /*
                        if (!notificationsByUser.containsKey(user.notificationEmail))
                            notificationsByUser.put((String)user.notificationEmail, [])
                        notificationsByUser[user.notificationEmail].add(generateEmailBody(id, triggered))
                        */
                        System.err.println("DO IT, MAKE A NOTIFICATION RECORD FOR INSTANCE " + id + " : " + triggered)
                    }
                }
            }
        }
    }

    /**
     * 'from' and 'until' are the instants of writing for the changed record (the previous and current versions)
     * 'user' is the user-data map for a user (which includes their selection of triggers).
     * 'library' is a library ("sigel") holding the instance in question.
     *
     * This function answers the question: Has 'user' requested to be notified of the occurred changes between
     * 'from' and 'until' for instances held by 'heldBy'?
     *
     * Returns the URIs of all triggered rules/triggers.
     */
    private List<String> changeMatchesAnyTrigger(String instanceId, Instant from, Instant until, Map user, String heldBy) {

        List<String> triggeredTriggers = []

        user.requestedNotifications.each { request ->

            // This stuff (the request) comes from a user, so we must be super paranoid about it being correctly formed.

            if (! request instanceof Map)
                return

            if (! request["heldBy"] instanceof String)
                return

            if (request["heldBy"] != heldBy)
                return

            if (! request["triggers"] instanceof List)
                return

            for (Object triggerObject : request["triggers"]) {
                if (! triggerObject instanceof String)
                    return
                String triggerUri = (String) triggerObject
                /*if (triggerIsTriggered(instanceId, from, until, triggerUri))
                    triggeredTriggers.add(triggerUri)*/
            }
        }

        return triggeredTriggers
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
        Document changed = whelk.getStorage().load(changedInstanceId)
        String mainTitle = Document._get(["@graph", 1, "hasTitle", 0, "mainTitle"], changed.data)
        if (mainTitle == null)
            mainTitle = "Ingen huvudtitel"

        String changeCategories = ""
        for (String categoryUri : triggeredCategories) {
            Document categoryDoc = whelk.getStorage().loadDocumentByMainId(categoryUri)
            String swedishLabel = Document._get(["@graph", 1, "prefLabelByLand", "sv"], categoryDoc.data)
            if (swedishLabel) {
                changeCategories += swedishLabel + " "
            } else {
                changeCategories += categoryUri + " "
            }
        }

        return "Instansbeskrivning har ändrats\n\tInstans: " + Document.BASE_URI.resolve(changedInstanceId) +
                "(" + mainTitle + ")\n\t" +
                "Ändring har skett med avseende på: " + changeCategories + "\n\n"
    }
}

package whelk.housekeeping

import org.simplejavamail.api.email.Email
import org.simplejavamail.api.mailer.Mailer
import org.simplejavamail.email.EmailBuilder
import org.simplejavamail.mailer.MailerBuilder
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import whelk.history.DocumentVersion
import whelk.util.PropertyLoader

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
@Log
class NotificationSender extends HouseKeeper {

    private final String STATE_KEY = "Email notifications"
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

    public String getName() {
        return "Notifications sender"
    }

    public String getStatusDescription() {
        return status
    }

    public void trigger() {
        // Build a multi-map of library -> list of settings objects for that library's users
        Map<String, List<Map>> heldByToUserSettings = new HashMap<>();
        {
            List<Map> allUserSettingStrings = whelk.getStorage().getAllUserData()
            for (Map settings : allUserSettingStrings) {
                if (!settings["notificationEmail"])
                    continue
                settings?.requestedNotifications.each { request ->
                    if (!request instanceof Map)
                        return
                    if (!request["heldBy"])
                        return

                    String heldBy = request["heldBy"]
                    if (!heldByToUserSettings.containsKey(heldBy))
                        heldByToUserSettings.put(heldBy, [])
                    List userSettingsForThisHeldBy = heldByToUserSettings[heldBy]
                    userSettingsForThisHeldBy.add(settings)
                }
            }
        }

        // Determine the time interval of changes for which to generate notifications.
        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS)) // Default to last 24h if first time.
        Map state = whelk.getStorage().getState(STATE_KEY)
        if (state && state.lastEmailTime)
            from = Timestamp.from( ZonedDateTime.parse( (String) state.lastEmailTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant() )
        Timestamp until = Timestamp.from(Instant.now())

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Fetch all changed IDs within the interval
            String sql = "SELECT id FROM lddb WHERE collection IN ('bib', 'auth') AND ( modified > ? AND modified <= ? );"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            // If both an instance and one of it's dependencies are affected within the same interval, we will
            // (without this check) try to generate notifications for said instance twice.
            Set affectedInstanceIDs = []
            Map<String, List<String>> notificationsByUser = new HashMap<>()
            while (resultSet.next()) {
                String id = resultSet.getString("id")
                generateNotificationsForChangedID(id, heldByToUserSettings, from.toInstant(),
                        until.toInstant(), affectedInstanceIDs, notificationsByUser)
            }

            notificationsByUser.keySet().each { email ->
                List<String> notifications = notificationsByUser.get(email)
                sendEmail(senderAddress, email, "Förändingsmeddelande", notifications.join("\n"))
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

    private void sendEmail(String sender, String recipient, String subject, String body) {
        Email email  = EmailBuilder.startingBlank()
                .to(recipient)
                .withSubject(subject)
                .from(sender)
                .withPlainText(body)
                .buildEmail()

        if (mailer) {
            log.info("Sending notification (cxz) email to " + recipient)
            mailer.sendMail(email)
        } else {
            log.info("Should now have sent notification (cxz) email to " + recipient + " but SMTP not configured.")
        }
    }

    /**
     * Based on the fact that 'id' has been updated, generate (if the change resulted in a ChangeNotice)
     * and collect notifications per user into 'notificationsByUser'
     */
    private void generateNotificationsForChangedID(String id, Map heldByToUserSettings,
                                                   Instant from, Instant until, Set affectedInstanceIDs,
                                                   Map<String, List<String>> notificationsByUser) {
        List<Tuple2<String, String>> dependers = whelk.getStorage().followDependers(id, ["itemOf"])
        dependers.add(new Tuple2(id, null)) // This ID too, not _only_ the dependers!
        dependers.each {
            String dependerID =  it[0]
            String dependerMainEntityType = whelk.getStorage().getMainEntityTypeBySystemID(dependerID)
            if (whelk.getJsonld().isSubClassOf(dependerMainEntityType, "Instance")) {
                // If we've not already sent a notification for this instance!
                if (!affectedInstanceIDs.contains(dependerID)) {
                    affectedInstanceIDs.add(dependerID)
                    generateNotificationsForAffectedInstance(dependerID, heldByToUserSettings, from,
                            until, notificationsByUser)
                }
            }
        }

    }

    /**
     * Generate notifications for an affected bibliographic instance. Beware: fromVersion and untilVersion may not be
     * _of this document_ (id), but rather of a document this instance depends on!
     */
    private void generateNotificationsForAffectedInstance(String id, Map heldByToUserSettings, Instant from,
                                                          Instant until,
                                                          Map<String, List<String>> notificationsByUser) {
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
                                "library": "https://libris.kb.se/library/Utb1",
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
                        if (!notificationsByUser.containsKey(user.notificationEmail))
                            notificationsByUser.put((String)user.notificationEmail, [])
                        notificationsByUser[user.notificationEmail].add(
                                "En instansbeskrivning har ändrats\n\tInstans: " + Document.BASE_URI.resolve(id) + "\n\tÄndrinskatergorier: " + triggered+"\n")
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
     * 'from' and 'until' for instances held by 'library'?
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
                if (triggerIsTriggered(instanceId, from, until, triggerUri))
                    triggeredTriggers.add(triggerUri)
            }
        }

        return triggeredTriggers
    }

    private boolean triggerIsTriggered(String instanceId, Instant before, Instant after, String triggerUri) {
        Document instanceAfterChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(after))

        // Populate 'changeNotes' with every note affecting this instance in the specified interval
        List<Map> changeNotes = new ArrayList<>();
        List notesOnInstance = (List) Document._get(["@graph", 0, "hasChangeNote"], instanceAfterChange.data)
        if (notesOnInstance != null)
        changeNotes.addAll(notesOnInstance)
        switch (triggerUri) {
            case "https://id.kb.se/changenote/primarytitle":
            case "https://id.kb.se/changenote/maintitle":
                historicEmbellish(instanceAfterChange, ["mainEntity", "hasTitle"], after, changeNotes)
                break
            case "https://id.kb.se/changenote/primarypublication":
            case "https://id.kb.se/changenote/serialtermination":
                historicEmbellish(instanceAfterChange, ["publication"], after, changeNotes)
                break
            case "https://id.kb.se/changenote/intendedaudience":
                historicEmbellish(instanceAfterChange, ["mainEntity", "intendedAudience"], after, changeNotes)
                break
            case "https://id.kb.se/changenote/ddcclassification":
            case "https://id.kb.se/changenote/sabclassification":
                historicEmbellish(instanceAfterChange, ["mainEntity", "classification"], after, changeNotes)
                break
            case "https://id.kb.se/changenote/serialrelation":
                historicEmbellish(instanceAfterChange, ["mainEntity", "precededBy", "succeededBy"], after, changeNotes)
                break
            /*case "https://id.kb.se/changenote/primarycontribution": {
                historicEmbellish(instanceAfterChange, ["mainEntity", "instanceOf", "contribution"], after, changeNotes)
            }*/
        }

        boolean matches = false
        filterChangeNotesNotInInterval(changeNotes, before, after)
        changeNotes.each { note ->
            note.category.each { category ->
                if (category["@id"] == triggerUri)
                    matches = true
            }
        }
        return matches
    }

    private void filterChangeNotesNotInInterval(List<Map> changeNotes, Instant before, Instant after) {
        changeNotes.removeAll { changeNote ->
            if (changeNote == null)
                return true
            if (!changeNote.atTime)
                return true
            Instant atTime = ZonedDateTime.parse( (String) changeNote.atTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant()
            if (atTime.isBefore(before) || atTime.isAfter(after))
                return true
            return false
        }
    }

    /**
     * This is a simplified/specialized from of 'embellish', for historic data and using only select properties.
     * The full general embellish code can not help us here, because it is based on the idea of cached cards,
     * which can (and must!) only cache the latest/current data for each card, which isn't what we need here
     * (we need to embellish older historic data).
     *
     * This function mutates docToEmbellish
     * This function also collects metadata ChangeNotes from embellished records.
     */
    private historicEmbellish(Document docToEmbellish, List<String> properties, Instant asOf, List<Map> changeNotes) {
        List graphListToEmbellish = docToEmbellish.data["@graph"]
        Set alreadyLoadedURIs = []

        for (int i = 0; i < properties.size(); ++i) {
            Set uris = findLinkedURIs(graphListToEmbellish, properties)
            uris.removeAll(alreadyLoadedURIs)
            if (uris.isEmpty())
                break

            Map<String, Document> linkedDocumentsByUri = whelk.bulkLoad(uris, asOf)
            linkedDocumentsByUri.each {
                List linkedGraphList = it.value.data["@graph"]
                if (linkedGraphList.size() > 1)
                    graphListToEmbellish.add(linkedGraphList[1])
                linkedGraphList[0]["hasChangeNote"].each { changeNote ->
                    changeNotes.add( (Map) changeNote )
                }
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

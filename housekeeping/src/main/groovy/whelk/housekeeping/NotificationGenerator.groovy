package whelk.housekeeping

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import whelk.history.DocumentVersion

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

@CompileStatic
@Log
class NotificationGenerator extends HouseKeeper {

    private static final int DAYS_TO_KEEP_NOTIFICATIONS = 10
    private String status = "OK"
    private Whelk whelk

    public NotificationGenerator(Whelk whelk) {
        this.whelk = whelk
    }

    public String getName() {
        return "Notifications generator"
    }

    public String getStatusDescription() {
        return status
    }

    public void trigger() {

        // Build a multi-map of library -> list of settings objects for that library's users
        Map<String, List<Map>> libraryToUsers = new HashMap<>();
        {
            List<Map> allUserSettingStrings = whelk.getStorage().getAllUserData()
            for (Map settings : allUserSettingStrings) {
                settings?.requestedNotifications.each { request ->
                    if (!request instanceof Map)
                        return
                    if (!request["library"])
                        return

                    String library = request["library"]
                    if (!libraryToUsers.containsKey(library))
                        libraryToUsers.put(library, [])
                    List userSettingsForThisLib = libraryToUsers[library]
                    userSettingsForThisLib.add(settings)
                }
            }
        }

        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            // Determine the time interval of changes for which to generate notices.
            // This interval, should generally be: From the last generated notice until now.
            // However, if there are no previously generated notices (near enough in time), use
            // now - [some pre set value], to avoid scanning the whole catalog.
            String sql = "SELECT MAX(created) FROM lddb__notifications;"
            statement = connection.prepareStatement(sql)
            resultSet = statement.executeQuery()
            Timestamp from = Timestamp.from(Instant.now().minus(DAYS_TO_KEEP_NOTIFICATIONS, ChronoUnit.DAYS))
            if (resultSet.next()) {
                Timestamp lastCreated = resultSet.getTimestamp(1)
                if (lastCreated && lastCreated.after(from))
                    from = lastCreated
            }
            Timestamp until = Timestamp.from(Instant.now())

            // Then fetch all changed IDs within that interval
            sql = "SELECT id FROM lddb WHERE collection IN ('bib', 'auth') AND ( modified BETWEEN ? AND ? );"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            while (resultSet.next()) {
                String id = resultSet.getString("id")
                generateNotificationsForChangedID(id, libraryToUsers, from.toInstant())
            }

            // Finally, clean out any notifications that are too old
            sql = "DELETE FROM lddb__notifications WHERE created < ?"
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, Timestamp.from(Instant.now().minus(DAYS_TO_KEEP_NOTIFICATIONS, ChronoUnit.DAYS)))
            statement.executeUpdate()

        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
        }
    }

    private void generateNotificationsForChangedID(String id, Map libraryToUsers, Instant since) {
        // "versions" come sorted by ascending modification time, so oldest version first.
        // We want to pick the "from version" (the base for which this notice details changes)
        // as the last saved version *before* the sought interval.
        DocumentVersion fromVersion = null
        List<DocumentVersion> versions = whelk.getStorage().loadDocumentHistory(id)
        for (DocumentVersion version : versions) {
            if (version.doc.getModifiedTimestamp().isBefore(since))
                fromVersion = version
        }
        if (fromVersion == null)
            return

        DocumentVersion untilVersion = versions.last()
        if (untilVersion == fromVersion)
            return

        List<Tuple2<String, String>> dependers = whelk.getStorage().followDependers(id, ["itemOf"])
        dependers.add(new Tuple2(id, null)) // This ID too, not _only_ the dependers!
        dependers.each {
            String dependerID =  it[0]
            String dependerMainEntityType = whelk.getStorage().getMainEntityTypeBySystemID(dependerID)
            if (whelk.getJsonld().isSubClassOf(dependerMainEntityType, "Instance")) {
                generateNotificationsForAffectedInstance(dependerID, libraryToUsers, fromVersion, untilVersion)
            }
        }

    }

    /**
     * Generate notice for a bibliographic instance. Beware: fromVersion and untilVersion may not be
     * _of this document_ (id), but rather of a document this instance depends on!
     */
    private void generateNotificationsForAffectedInstance(String id, Map libraryToUsers, DocumentVersion fromVersion, DocumentVersion untilVersion) {
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(id)
        for (String library : libraries) {
            List<Map> users = (List<Map>) libraryToUsers[library]
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
                                    "https://id.kb.se/notificationtriggers/sab",
                                    "https://id.kb.se/notificationtriggers/primarycontribution"
                                ]
                            }
                        ]
                    }*/

                    List<String> triggered = changeMatchesAnyTrigger(
                            id, fromVersion.versionWriteTime.toInstant(),
                            untilVersion.versionWriteTime.toInstant(), user, library)
                    if (triggered) {
                        whelk.getStorage().insertNotification(untilVersion.versionID, user["id"].toString(), ["triggered" : triggered]) // TODO: Use until as created time!!
                        System.err.println("STORED NOTICE FOR USER " + user["id"].toString() + " version: " + untilVersion.versionID)
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
    private List<String> changeMatchesAnyTrigger(String instanceId, Instant from, Instant until, Map user, String library) {

        List<String> triggeredTriggers = []

        user.requestedNotifications.each { request ->

            // This stuff (the request) comes from a user, so we must be super paranoid about it being correctly formed.

            if (! request instanceof Map)
                return

            if (! request["library"] instanceof String)
                return

            if (request["library"] != library)
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

    /**
     * Do changes to the graph between times 'from' and 'until' affect 'instanceId' in such a way as to qualify 'triggerUri' triggered?
     */
    private boolean triggerIsTriggered(String instanceId, Instant from, Instant until, String triggerUri) {

        // Load the two versions (old/new) of the instance
        Document instanceBeforeChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(from))
        // If a depender is created after a dependency, it will ofc not have existed at the original writing time
        // of the dependency, if so, simply load the first available version of the depender.
        if (instanceBeforeChange == null)
            instanceBeforeChange = whelk.getStorage().load(instanceId, "0")
        Document instanceAfterChange = whelk.getStorage().loadAsOf(instanceId, Timestamp.from(until))

        switch (triggerUri) {

            case "https://id.kb.se/notificationtriggers/primarycontribution": {
                // Embellish both the new and old versions with historic dependencies from their respective times
                historicEmbellish(instanceBeforeChange, ["instanceOf", "contribution", "agent"], from, 2)
                historicEmbellish(instanceAfterChange, ["instanceOf", "contribution", "agent"], until, 2)

                Object contributionsBefore = Document._get(["mainEntity", "instanceOf", "contribution"], instanceBeforeChange.data)
                Object contributionsAfter = Document._get(["mainEntity", "instanceOf", "contribution"], instanceAfterChange.data)

                if (contributionsBefore == null || contributionsAfter == null || ! contributionsBefore instanceof List || !contributionsAfter instanceof List)
                    return false

                for (Object contrBefore : contributionsBefore) {
                    for (Object contrAfter : contributionsAfter) {
                        if (contrBefore["@type"].equals("PrimaryContribution") && contrAfter["@type"].equals("PrimaryContribution") ) {
                            if (!contrBefore.equals(contrAfter))
                                return true
                        }
                    }
                }
                break
            }

        }
        return false
    }

    /**
     * This is a simplified/specialized from of 'embellish', for historic data and using only select properties.
     * The full general embellish code can not help us here, because it is based on the idea of cached cards,
     * which can (and must!) only cache the latest/current data for each card, which isn't what we need here
     * (we need to embellish older historic data).
     *
     * 'passes' means the number of times to collect URIs and expand the document. In practice it is the limit
     * of "number of links" away we can look.
     *
     * This function mutates docToEmbellish
     */
    private historicEmbellish(Document docToEmbellish, List<String> properties, Instant asOf, int passes) {
        List graphListToEmbellish = docToEmbellish.data["@graph"]

        for (int i = 0; i < passes; ++i) {
            Set uris = findLinkedURIs(graphListToEmbellish, properties)

            Map<String, Document> linkedDocumentsByUri = whelk.bulkLoad(uris, asOf)
            linkedDocumentsByUri.each {
                List linkedGraphList = it.value.data["@graph"]
                if (linkedGraphList.size() > 1)
                    graphListToEmbellish.add(linkedGraphList[1])
            }
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

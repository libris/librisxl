package whelk.housekeeping

import whelk.Document
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

                    List<String> triggered = changeMatchesAnyTrigger(id, fromVersion, untilVersion, user, library)
                    if (triggered) {
                        whelk.getStorage().insertNotification(untilVersion.versionID, user["id"].toString(), ["triggered" : triggered])
                        System.err.println("STORED NOTICE FOR USER " + user["id"].toString() + " version: " + untilVersion.versionID)
                    }
                }
            }
        }
    }

    /**
     * '*version' parameters are the two relevant versions (before and after).
     * 'user' is the user-data map for a user (which includes their selection of triggers).
     * 'library' is a library ("sigel") holding the instance in question.
     *
     * This function answers the question: Has 'user' requested to be notified of the change between
     * 'fromVersion' and 'untilVersion' for instances held by 'library'?
     *
     * Returns the URIs of all triggered rules/triggers.
     */
    private List<String> changeMatchesAnyTrigger(String instanceId, DocumentVersion fromVersion, DocumentVersion untilVersion, Map user, String library) {

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
                if (triggerIsTriggered(instanceId, fromVersion, untilVersion, triggerUri))
                    triggeredTriggers.add(triggerUri)
            }
        }

        return triggeredTriggers
    }

    /**
     * Do the changes between 'fromVersion' and 'untilVersion' affect 'instanceId' in such a way as to qualify 'triggerUri' triggered?
     */
    private boolean triggerIsTriggered(String instanceId, DocumentVersion fromVersion, DocumentVersion untilVersion, String triggerUri) {
        switch (triggerUri) {
            case "https://id.kb.se/notificationtriggers/primarycontribution":
                //whelk.getStorage().load
                //whelk.embellish()
                //fromVersion.
                //Document from = fromVersion.doc.clone()
                //Document until = untilVersion.doc.clone()

                Document affectedInstance = whelk.getStorage().loadAsOf(instanceId, fromVersion.versionWriteTime)

                // If a depender is created after a dependency, it will ofc not have existed at the original writing time
                // of the dependency, if so, simply load the first available version of the depender.
                if (affectedInstance == null)
                    affectedInstance = whelk.getStorage().load(instanceId, "0")

                //System.err.println("**** Loaded historic version of INSTANCE: " + affectedInstance.getDataAsString())

                historicEmbellish(affectedInstance, ["instanceOf", "contribution", "agent"], fromVersion.versionWriteTime.toInstant())
                return true
                break
        }
        return false
    }

    /**
     * This is a simplified/specialized from of 'embellish', for historic data and using only select properties.
     * The full general embellish code can not help us here, because it is based on the idea of cached cards,
     * which can (and must!) only cache the latest/current data for each card, which isn't what we need here
     * (we need to embellish older historic data).
     */
    private historicEmbellish(Document doc, List<String> properties, Instant asOf) {
        List graphList = doc.data["@graph"]

        System.err.println("**** WILL NOW SCAN FOR LINKS:\n\t" + doc.getDataAsString())

        Set uris = findLinkedURIs(graphList, properties)

        System.err.println("\tFound links (with chosen properties): " + uris)

        Map<String, Document> linkedDocumentsByUri = whelk.bulkLoad(uris, asOf)

        //System.err.println("Was able to fetch historic data for: " + linkedDocumentsByUri.keySet())
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
        System.err.println("\tCHECK getLinkIfAny with " + node)
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

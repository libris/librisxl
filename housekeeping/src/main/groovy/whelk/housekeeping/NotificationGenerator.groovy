package whelk.housekeeping

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
                settings?.requestedNotices.each { request ->
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
            Timestamp from = Timestamp.from(Instant.now().minus(2, ChronoUnit.DAYS))
            if (resultSet.next()) {
                Timestamp lastCreated = resultSet.getTimestamp(1)
                if (lastCreated && lastCreated.after(from))
                    from = lastCreated
            }
            Timestamp until = Timestamp.from(Instant.now())

            // Then fetch all changed IDs within that interval
            sql = "SELECT id FROM lddb WHERE collection IN ('bib', 'auth') AND ( modified BETWEEN ? AND ? );";
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setTimestamp(2, until)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            while (resultSet.next()) {
                String id = resultSet.getString("id")
                generateNoticesForChangedID(id, libraryToUsers, from.toInstant())
            }

        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
        }
    }

    private void generateNoticesForChangedID(String id, Map libraryToUsers, Instant since) {
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
                generateNoticesForAffectedInstance(dependerID, libraryToUsers, fromVersion, untilVersion)
            }
        }

    }

    /**
     * Generate notice for a bibliographic instance. Beware: fromVersion and untilVersion may not be
     * _of this document_ (id), but rather of a document this instance depends on!
     */
    private void generateNoticesForAffectedInstance(String id, Map libraryToUsers, DocumentVersion fromVersion, DocumentVersion untilVersion) {
        List<String> libraries = whelk.getStorage().getAllLibrariesHolding(id)
        for (String library : libraries) {
            List<Map> users = (List<Map>) libraryToUsers[library]
            if (users) {
                for (Map user : users) {

                    /*
                    user is a map looking something like this:
                    {
                        "id": "sldknfslkdnsdlkgnsdkjgnb"
	                    "requestedNotices": [
			                {"library": "https://libris.kb.se/library/Utb1", "trigger": ["@graph", 1, "contribution", "@type=PrimaryContribution", "agent"]},
			                {"library": "https://libris.kb.se/library/Utb2", "trigger": ["@graph", 1, "instanceOf", "hasTitle", "*", "mainTitle"]}
			                ]
			        }
                     */
                    //System.err.println("" + user["id"].toString() + " has requested updates for " + library)

                    /*List<DocumentVersion> relevantVersions = []
                    relevantVersions.add(fromVersion)
                    relevantVersions.add(untilVersion)*/

                    /*System.err.println("Was changed: " + id + " spans: " + fromVersion.doc.getModified() + " -> " + untilVersion.doc.getModified())

                    History history = new History(relevantVersions, whelk.getJsonld())

                    Map changes = history.m_changeSetsMap
                    System.err.println(changes)*/

                    Map changes = ["STILL":"TODO"]

                    if (changeMatchesAnyTrigger(fromVersion, untilVersion, user)) {
                        whelk.getStorage().insertNotification(untilVersion.versionID, user["id"].toString(), changes)
                        System.err.println("STORED NOTICE FOR USER " + user["id"].toString() + " version: " + untilVersion.versionID)
                    }
                }
            }
        }
    }

    /**
     * Parameters are the two relevant versions (before and after), and user is
     * the user-data map for a user (which includes their selection of triggers)
     */
    private boolean changeMatchesAnyTrigger(DocumentVersion fromVersion, DocumentVersion untilVersion, Map user) {
        return true
    }

}

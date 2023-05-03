package whelk.housekeeping

import whelk.Document
import whelk.Whelk
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import whelk.history.DocumentVersion
import whelk.history.History

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import static whelk.util.Jackson.mapper

@CompileStatic
@Log
class CXZGenerator extends HouseKeeper {

    private String status = "OK"
    private Whelk whelk

    public CXZGenerator(Whelk whelk) {
        this.whelk = whelk
    }

    public String getName() {
        return "CXZ notifier generator"
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
            String sql = "SELECT MAX(created) FROM lddb__notices;"
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
                generateNoticesForChange(id, libraryToUsers, from.toInstant())
            }

        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
        }
    }

    private void generateNoticesForChange(String id, Map libraryToUsers, Instant since) {
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

        List<DocumentVersion> relevantVersions = []
        relevantVersions.add(fromVersion)
        relevantVersions.add(untilVersion)

        System.err.println("Was changed: " + id + " spans: " + fromVersion.doc.getModified() + " -> " + untilVersion.doc.getModified())

        History history = new History(relevantVersions, whelk.getJsonld())

        Map changes = history.m_changeSetsMap
        System.err.println(changes)
        //System.err.println("added:\n\t" + changes.addedPaths)
        //System.err.println("removed:\n\t" + changes.removedPaths)

        // TODO: Om id är i 'auth' gör istället allt nedan för beroende (och embellished!) instanser

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

                    if (changeMatchesAnyTrigger(fromVersion, untilVersion, user)) {
                        whelk.getStorage().insertNotice(untilVersion.versionID, user["id"].toString(), changes)
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


    //zonedFrom = ZonedDateTime.parse(from);
    //zonedUntil = ZonedDateTime.parse(until);
    /*public void generate(ZonedDateTime zonedFrom, ZonedDateTime zonedUntil) throws SQLException {

        Connection connection;
        PreparedStatement statement;
        ResultSet resultSet;

        connection = whelk.getStorage().getOuterConnection();
        try {
            String sql = "SELECT id FROM lddb WHERE collection IN ('bib', 'auth', 'hold') AND ( modified BETWEEN ? AND ? );";
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            statement.setTimestamp(1, new Timestamp(zonedFrom.toInstant().getEpochSecond() * 1000L));
            statement.setTimestamp(2, new Timestamp(zonedUntil.toInstant().getEpochSecond() * 1000L));
            statement.setFetchSize(512);
            resultSet = statement.executeQuery();
        } catch (Throwable e) {
            connection.close();
            throw e;
        }
    }*/


}


/*
       if (fromVersion < 0 || fromVersion >= untilVersion)
        return // error out

    // We don't need all versions, just specifically the starting and end point of the
    // sought interval.
    List<DocumentVersion> versions = whelk.getStorage().loadDocumentHistory(id)
    List<DocumentVersion> relevantVersions = []
    relevantVersions.add(versions.get(fromVersion))
    relevantVersions.add(versions.get(untilVersion))

    History history = new History(relevantVersions, whelk.getJsonld())
    Map changes = history.m_changeSetsMap.changeSets[1]
    System.err.println("added:\n\t" + changes.addedPaths)
    System.err.println("removed:\n\t" + changes.removedPaths)
 */


/*
void doGet(HttpServletRequest request, HttpServletResponse response) {
{
    Map userInfo = request.getAttribute("user")
    if (!isValidUserWithPermission(request, response, userInfo))
        return

    String id = "${userInfo.id}".digest(ID_HASH_FUNCTION)
    String data = whelk.getUserData(id) ?: upgradeOldEmailBasedEntry(userInfo) ?: "{}"
}
 */

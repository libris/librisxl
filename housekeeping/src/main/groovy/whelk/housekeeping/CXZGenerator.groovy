package whelk.housekeeping

import whelk.Whelk
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log

@CompileStatic
@Log
class CXZGenerator extends HouseKeeper {

    public CXZGenerator(Whelk whelk) {

    }

    public String getName() {
        return "CXZ notifier generator"
    }

    public String getStatusDescription() {
        return "OK"
    }

    public void trigger() {
        System.err.println("  ** INTERNAL TICK **")
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

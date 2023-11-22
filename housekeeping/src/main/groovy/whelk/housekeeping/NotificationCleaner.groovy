package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import whelk.Whelk

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

@CompileStatic
@Log4j2
class NotificationCleaner extends HouseKeeper {
    private static final int DAYS_TO_KEEP_INACTIVE = 60
    private String status = "OK"
    private final Whelk whelk

    public NotificationCleaner(Whelk whelk) {
        this.whelk = whelk
    }

    public String getName() {
        return "Notifications cleaner"
    }

    public String getStatusDescription() {
        return status
    }

    public String getCronSchedule() {
        return "0 4 * * *"
    }

    public void trigger() {
        Connection connection
        PreparedStatement statement
        ResultSet resultSet

        connection = whelk.getStorage().getOuterConnection()
        connection.setAutoCommit(false)
        try {
            Timestamp from = Timestamp.from(Instant.now().minus(DAYS_TO_KEEP_INACTIVE, ChronoUnit.DAYS))

            Set<String> typesToClean = whelk.getJsonld().getSubClasses("AdministrativeNotice")
            typesToClean.add("AdministrativeNotice")

            String sql = "SELECT id FROM lddb WHERE collection = 'none' AND data#>>'{@graph,1,@type}' = ANY (?) AND modified < ? AND created < ?;"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setArray(1, connection.createArrayOf("TEXT", typesToClean as String[]))
            statement.setTimestamp(2, from)
            statement.setTimestamp(3, from)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()
            while (resultSet.next()) {
                String id = resultSet.getString("id")
                whelk.remove(id, "NotificationGenerator", "SEK")
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString()
            throw e
        } finally {
            connection.close()
        }
    }
}

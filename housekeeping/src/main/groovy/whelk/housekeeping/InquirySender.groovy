package whelk.housekeeping

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2
import whelk.Whelk

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
class InquirySender extends HouseKeeper {
    private final String STATE_KEY = "CXZ inquiry email sender"
    private String status = "OK"
    private final Whelk whelk

    public InquirySender(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    String getName() {
        return "Inquiry sender"
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

        Timestamp from = Timestamp.from(Instant.now().minus(1, ChronoUnit.DAYS))
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
            String sql = "SELECT modified, data FROM lddb WHERE deleted = false AND data#>>'{@graph,1,@type}' IN ('InquiryAction', 'ChangeNotice') AND modified > ?;"
            connection.setAutoCommit(false)
            statement = connection.prepareStatement(sql)
            statement.setTimestamp(1, from)
            statement.setFetchSize(512)
            resultSet = statement.executeQuery()

            while (resultSet.next()) {
                String data = resultSet.getString("data")

                // TODO: Send email!

                Instant lastChangeObservationForInstance = resultSet.getTimestamp("modified").toInstant()
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
}

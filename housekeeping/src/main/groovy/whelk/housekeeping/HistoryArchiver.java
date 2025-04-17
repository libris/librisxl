package whelk.housekeeping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class HistoryArchiver extends HouseKeeper {
    private static final int CHANGELOG_DAYS_RETENTION = 120;
    private String status = "OK";
    private final Whelk whelk;
    private final static Logger logger = LogManager.getLogger(HistoryArchiver.class);

    public HistoryArchiver(Whelk whelk) {
        this.whelk = whelk;
    }

    public String getName() {
        return "History archiver";
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return "0 3 * * *";
    }

    public void trigger() {
        status = "OK";

        // Clean out old data from the change log (we're only keeping a few months of it)
        String sql = "DELETE FROM lddb__change_log WHERE time < ?";
        try (Connection connection = whelk.getStorage().getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp( Instant.now().minus(CHANGELOG_DAYS_RETENTION, ChronoUnit.DAYS).toEpochMilli() ));
            statement.execute();
        } catch (SQLException e) {
            logger.error("Failed to clean change log.", e);
            status = e.getMessage();
            throw new RuntimeException(e);
        }

        // TODO: When compressed history comes live, archive deleted history here.
    }
}

package whelk.housekeeping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;

public class ExportSizePredictor extends HouseKeeper {

    private String status = "OK";
    private final Whelk whelk;
    private final Logger logger = LogManager.getLogger(this.getClass());

    public ExportSizePredictor(Whelk whelk) {
        this.whelk = whelk;
    }

    public String getName() {
        return "Export size predictor";
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return "15 14,16,18 * * *";
    }

    public void trigger() {

        int items = countChanged("Item");
        int instances = countChanged("Instance");
        int works = countChanged("Work");
        int topics = countChanged("Topic");
        int agents = countChanged("Agent");

        if (items > 350000 || instances > 50000 || works > 5000 || topics > 250 || agents > 300) {
            logger.warn("Potentially too large export going tonight, changed so far today: "
                    + items + " items, " + instances + " instances, " + works + " works, "
                    + topics + " topics, " + agents + " agents." );
        }
    }

    private int countChanged(String baseType) {
        Timestamp from = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.DAYS));
        String sql = "SELECT count(id) FROM lddb WHERE modified > ? AND data#>>'{@graph,1,@type}' IN (€);";

        Set<String> types = whelk.getJsonld().getSubClasses(baseType);
        types.add(baseType);
        String replacement = "'" + String.join("', '",  types) + "'";
        String concreteQuery = sql.replace("€", replacement);

        try (Connection connection = whelk.getStorage().getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(concreteQuery)) {
            statement.setTimestamp(1, from);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1);
                }
            }
        } catch (Throwable e) {
            status = "Failed with:\n" + e + "\nat:\n" + e.getStackTrace().toString();
            throw new RuntimeException(e);
        }
        return 0;
    }
}

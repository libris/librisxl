package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

public class Indexing {

    // There is to be only one of these. Using more than one thread for indexing potentially breaks made assumptions
    // with regard to indexing-order of records, _which matters_.
    static Thread worker;
    private final static String INDEXER_STATE_KEY = "ElasticIndexer";
    private final static Logger logger = LogManager.getLogger(Indexing.class);

    /**
     * Index all changes since last invocation of this function
     */
    private static void iterate(PostgreSQLComponent psql, ElasticSearch elastic) throws SQLException {
        Map storedIndexerState = psql.getState(INDEXER_STATE_KEY);
        if (storedIndexerState == null){
            resetStateToNow(psql);
            storedIndexerState = psql.getState(INDEXER_STATE_KEY);
        }
        long lastIndexedChangeNumber = (Long) storedIndexerState.get("lastIndexed");

        String sql = "SELECT * FROM lddb__change_log WHERE changenumber > ? ORDER BY changenumber ASC LIMIT 500";
        try (Connection connection = psql.getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);
            statement.setFetchSize(500);

            statement.setLong(1, lastIndexedChangeNumber);
            ResultSet resultSet = statement.executeQuery();
            Long changeNumber = 0L;
            while (resultSet.next()) {
                changeNumber = resultSet.getLong("changenumber");
                String id = resultSet.getString("id");
                Instant modificationInstant = resultSet.getTimestamp("time").toInstant();
                int resultingVersion = resultSet.getInt("resulting_record_version");

                System.err.println("Now want to reindex: " + id + " ch-nr: " + changeNumber + " recordv: " + resultingVersion);
            }

            if (changeNumber > lastIndexedChangeNumber) {
                psql.putState(INDEXER_STATE_KEY, Map.of("lastIndexed", changeNumber));
            }
        }

    }

    /**
     * This resets the state of the Indexing code to "now", as in:
     * "forget whatever you thought you had left to, you are just indexing new changes from *now* and forward."
     *
     * This should be called whenever a full reindexing has been done.
     */
    public synchronized static void resetStateToNow(PostgreSQLComponent psql) throws SQLException {
        String sql = """
        INSERT INTO lddb__state (key, value)
        SELECT 'ElasticIndexer', jsonb_build_object( 'lastIndexed', MAX(changenumber) ) FROM lddb__change_log
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value;
        """.stripIndent();

        try (Connection connection = psql.getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }
    }

    /**
     * Run in background, and index data continually as it changes
     */
    public synchronized static void start(PostgreSQLComponent psql, ElasticSearch elastic) {
        if (worker != null)
            return;

        worker = Thread.ofPlatform().name("Whelk elastic indexing").unstarted( new IndexingRunnable(psql, elastic) );
        worker.start();
    }

    // The necessary machinery for invoking iterate() at a suitable cadence
    private static class IndexingRunnable implements Runnable {
        private PostgreSQLComponent psql;
        ElasticSearch elastic;

        public IndexingRunnable(PostgreSQLComponent psql, ElasticSearch elastic) {
            this.psql = psql;
            this.elastic = elastic;
        }

        public void run() {
            while (true) {

                try {
                    iterate(psql, elastic);

                    // Don't run hot, wait a little before the next pass.
                    try {
                        Thread.sleep(100); // 0.1 seconds
                    } catch (InterruptedException ie) { /* ignore */ }
                } catch (Exception e) {
                    // Catch all, and wait a little before retrying. This thread should never be allowed to crash completely.
                    logger.error("Unexpected exception while indexing.", e);
                    try {
                        Thread.sleep(120 * 1000); // 2 minutes
                    } catch (InterruptedException ie) { /* ignore */ }
                }

            }
        }
    }
}

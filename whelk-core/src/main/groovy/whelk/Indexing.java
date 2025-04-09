package whelk;

import com.google.common.collect.Iterables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;

import static whelk.FeatureFlags.Flag.INDEX_BLANK_WORKS;

public class Indexing {

    // There is to be only one of these. Using more than one thread for indexing potentially breaks made assumptions
    // with regard to indexing-order of records, _which matters_.
    static Thread worker;
    private final static String INDEXER_STATE_KEY = "ElasticIndexer";
    private final static Logger logger = LogManager.getLogger(Indexing.class);
    private static Instant lastBehindMessageAt = Instant.EPOCH;

    /**
     * Index all changes since last invocation of this function
     *
     * returns false if there was nothing to index, true otherwise
     */
    private static boolean iterate(Whelk whelk) throws SQLException {
        PostgreSQLComponent psql = whelk.getStorage();
        Map storedIndexerState = psql.getState(INDEXER_STATE_KEY);
        if (storedIndexerState == null){
            resetStateToNow(psql);
            storedIndexerState = psql.getState(INDEXER_STATE_KEY);
        }
        long lastIndexedChangeNumber = Long.parseLong( (String) storedIndexerState.get("lastIndexed") );

        String sql = "SELECT * FROM lddb__change_log WHERE changenumber > ? ORDER BY changenumber ASC LIMIT 500";
        try (Connection connection = psql.getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            connection.setAutoCommit(false);
            statement.setFetchSize(500);

            statement.setLong(1, lastIndexedChangeNumber);
            ResultSet resultSet = statement.executeQuery();
            Long indexedChangeNumber = 0L;
            if (!resultSet.isBeforeFirst()) {
                return false;
            }
            while (resultSet.next()) {
                Long changeNumber = resultSet.getLong("changenumber");
                String id = resultSet.getString("id");
                Instant modificationInstant = resultSet.getTimestamp("time").toInstant();
                int resultingVersion = resultSet.getInt("resulting_record_version");
                boolean skipIndexDependers = resultSet.getBoolean("skipindexdependers");

                long minutesBehind = modificationInstant.until(Instant.now(), ChronoUnit.MINUTES);
                if (minutesBehind >= 15 && lastBehindMessageAt.until(Instant.now(), ChronoUnit.MINUTES) >= 30) {
                    lastBehindMessageAt = Instant.now();
                    logger.error("Elastic indexing is currently " + minutesBehind + " minutes behind. The next change to index is: " + changeNumber +
                            " (" + id + "). If this number is the same between two of these messages, it means that indexing is stuck on this change " +
                            "and cannot proceed until indexing it becomes possible. If you (in an emergency) need to proceed without indexing " +
                            "this change, do the following in the database: \"DELETE FROM lddb__change_log WHERE changenumber = " + changeNumber + ";\" " +
                            "No data will be lost (the log is temporary). But be aware: The inconsistency in the search index is now on YOU and will " +
                            "remain until the record is resaved or a full reindexing is done.");
                }

                try {
                    List<Document> versions = whelk.getStorage().loadAllVersions(id);
                    if (resultingVersion == 0)
                        whelk.elastic.index(versions.getFirst(), whelk);
                    else {
                        Document updated = versions.get(resultingVersion);
                        Document preUpdateDoc = versions.get(resultingVersion - 1);

                        //System.err.println("Now want to reindex: " + id + " ch-nr: " + changeNumber + " recordv: " + resultingVersion);
                        //System.err.println("data to index:\n\t" + updated.getDataAsString() + "\n");
                        //System.err.println("previous version:\n\t" + preUpdateDoc.getDataAsString() + "\n\n");
                        reindexUpdated(updated, preUpdateDoc, skipIndexDependers, whelk);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to index " + id + ", will try again.", e);
                    // When we fail, wait a little before trying again.
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException ie) { /* ignore */ }
                    break; // out of the while, without updating indexedChangeNumber
                }
                indexedChangeNumber = changeNumber;
            }

            if (indexedChangeNumber > lastIndexedChangeNumber) {
                psql.putState(INDEXER_STATE_KEY, Map.of("lastIndexed", ""+indexedChangeNumber));
            }
        }

        return true;
    }

    private static void reindexUpdated(Document updated, Document preUpdateDoc, boolean skipIndexDependers, Whelk whelk) {
        whelk.elastic.index(updated, whelk);
        if (whelk.getFeatures().isEnabled(INDEX_BLANK_WORKS)) {
            Set<String> removedIDs = preUpdateDoc.getVirtualRecordIds();
            removedIDs.removeAll(updated.getVirtualRecordIds());
            for (String removedID : removedIDs) {
                whelk.elastic.remove(removedID);
            }

            Set<String> existingIDs = updated.getVirtualRecordIds();
            for (String existingID : existingIDs) {
                whelk.elastic.index(updated.getVirtualRecord(existingID), whelk);
            }
        }
        if (hasChangedMainEntityId(updated, preUpdateDoc)) {
            reindexAllLinks(updated.getShortId(), whelk);
        } else if (!skipIndexDependers) {
            reindexAffected(updated, preUpdateDoc.getExternalRefs(), updated.getExternalRefs(), whelk);
        }
    }

    private static boolean hasChangedMainEntityId(Document updated, Document preUpdateDoc) {
        List<String> preMainEntityIDs = preUpdateDoc.getThingIdentifiers();
        List<String> postMainEntityIDs = updated.getThingIdentifiers();

        if (!postMainEntityIDs.isEmpty() && !preMainEntityIDs.isEmpty()){
            return postMainEntityIDs.getFirst().equals(preMainEntityIDs.getFirst());
        }
        return false;
    }

    private static void reindexAllLinks(String id, Whelk whelk) {
        SortedSet<String> links = whelk.getStorage().getDependencies(id);
        links.addAll(whelk.getStorage().getDependers(id));
        bulkIndex(links, whelk);
    }

    private static void bulkIndex(Iterable<String> ids, Whelk whelk) {
        for (List a : Iterables.partition(ids, 100)) {
            Collection<Document> docs = whelk.bulkLoad(a).values();
            whelk.elastic.bulkIndex(docs, whelk, false);
        }
    }

    private static void reindexAffected(Document document, Set<Link> preUpdateLinks, Set<Link> postUpdateLinks, Whelk whelk) {
        Set<Link> addedLinks = new HashSet<>(postUpdateLinks);
        addedLinks.removeAll(preUpdateLinks);
        Set<Link> removedLinks = new HashSet<>(preUpdateLinks);
        removedLinks.removeAll(postUpdateLinks);

        for (Link link : removedLinks) {
            String id = whelk.getStorage().getSystemIdByIri(link.getIri());
            if (id != null) {
                whelk.elastic.decrementReverseLinks(id, link.getRelation());
            }
        }

        for (Link link : addedLinks) {
                String id = whelk.getStorage().getSystemIdByIri(link.getIri());
            if (id != null) {
                Document doc = whelk.getStorage().load(id);
                List<String> lenses = Arrays.asList("chips", "cards", "full");
                List<String> reverseRelations = new ArrayList<>();
                for (String lens : lenses) {
                    reverseRelations.addAll ( whelk.getJsonld().getInverseProperties(doc.data, lens) );
                }

                if (reverseRelations.contains(link.getRelation())) {
                    // we added a link to a document that includes us in its @reverse relations, reindex it
                    whelk.elastic.index(doc, whelk);
                    // that document may in turn have documents that include it, and by extension us in their
                    // @reverse relations. Reindex them. (For example item -> instance -> work)
                    // TODO this should be calculated in a more general fashion. We depend on the fact that indexed
                    // TODO docs are embellished one level (cards, chips) -> everything else must be integral relations
                    reindexAffectedReverseIntegral(doc, whelk);
                } else {
                    // just update link counter
                    whelk.elastic.incrementReverseLinks(id, link.getRelation());
                }
            }
        }

        if (whelk.getStorage().isCardChangedOrNonexistent(document.getShortId())) {
            List<String> documentIDs = document.getThingIdentifiers();
            documentIDs.addAll(document.getRecordIdentifiers());
            bulkIndex(whelk.elastic.getAffectedIds(documentIDs), whelk);
        }
    }

    private static void reindexAffectedReverseIntegral(Document reIndexedDoc, Whelk whelk) {
        Set<Link> externalReferences = JsonLd.getExternalReferences(reIndexedDoc.data);
        for (Link link : externalReferences) {
                String p = link.property();
            if (whelk.getJsonld().isIntegral(whelk.getJsonld().getInverseProperty(p))) {
                String id = whelk.getStorage().getSystemIdByIri(link.getIri());
                Document doc = whelk.getStorage().load(id);

                List<String> lenses = Arrays.asList("chips", "cards", "full");
                List<String> reverseRelations = new ArrayList<>();
                for (String lens : lenses) {
                    reverseRelations.addAll ( whelk.getJsonld().getInverseProperties(doc.data, lens) );
                }

                if (reverseRelations.contains(p)) {
                    whelk.elastic.index(doc, whelk);
                }
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
        SELECT 'ElasticIndexer', jsonb_build_object( 'lastIndexed', MAX(changenumber)::text ) FROM lddb__change_log
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value;
        """.stripIndent();

        try (Connection connection = psql.getOuterConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }

        // At the one first start of this, the above 'MAX(changenumber)' will be null, as there are no
        // logged changenumbers yet. To cover this case, set an initial 0 explicitly.
        Map storedIndexerState = psql.getState(INDEXER_STATE_KEY);
        if (storedIndexerState != null && storedIndexerState.get("lastIndexed") == null) {
            psql.putState(INDEXER_STATE_KEY, Map.of("lastIndexed", "0"));
        }
    }

    /**
     * Run in background, and index data continually as it changes
     */
    public synchronized static void start(Whelk whelk) {
        if (worker != null)
            return;

        worker = Thread.ofPlatform().name("Whelk elastic indexing").unstarted( new IndexingRunnable(whelk) );
        worker.start();
    }

    // The necessary machinery for invoking iterate() at a suitable cadence
    private static class IndexingRunnable implements Runnable {
        Whelk whelk;

        public IndexingRunnable(Whelk whelk) {
            this.whelk = whelk;
        }

        public void run() {
            while (true) {

                try {
                    if (!iterate(whelk)) {
                        // If there was nothing to index, don't run hot! Wait a little before the next pass!
                        try {
                            Thread.sleep(200); // 0.2 seconds
                        } catch (InterruptedException ie) { /* ignore */ }
                    }
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

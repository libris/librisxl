package whelk.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whelk.Document;
import whelk.Link;
import whelk.Whelk;
import whelk.util.BlockingThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Gatherers;

import static whelk.FeatureFlags.Flag.INDEX_BLANK_WORKS;

/**
 * Indexes documents created/updated/removed by DatasetImporter instead of Whelk doing it after each
 * and every write (so when this is used, Whelk.skipIndex must set to true).
 * 
 * Documents are indexed in bulk in the background. Affected things that must be reindexed are collected
 * and then reindexed with flush() which must be called at the end.
 * 
 * NOTE: Must be kept in sync with Whelk.reindexUpdated() and Whelk.reindexAffected().
 * TODO, maybe: refactor/deduplicate, create a shared whelk-core class for figuring out what must be reindexed?
 */
public class DatasetIndexer {
    private static final Logger log = LoggerFactory.getLogger(DatasetIndexer.class);

    static final int BATCH_SIZE = 100;
    static final int AFFECTED_IDS_BATCH_SIZE = 200;

    private final Whelk whelk;
    private final BlockingThreadPool.SimplePool threadPool;

    private List<String> pendingBatch = new ArrayList<>();
    private int writtenCount = 0;

    private final Set<String> toRemove = new HashSet<>();
    private final Map<String, Set<String>> addedLinks = new HashMap<>();
    private final Set<String> removedLinkTargets = new HashSet<>();
    private final Set<String> changedCardIris = new HashSet<>();
    private final Set<String> changedMainEntityIds = new HashSet<>();
    // System IDs of other documents to reindex
    private final Set<String> toReindex = new HashSet<>();

    public DatasetIndexer(Whelk whelk, int numThreads) {
        this.whelk = whelk;
        this.threadPool = BlockingThreadPool.simplePool(numThreads);
    }

    public void created(Document doc) {
        written(doc.getShortId());
        if (!whelk.isSkipIndexDependers()) {
            linksChanged(doc, Set.of(), doc.getExternalRefs());
        }
    }

    public void updated(Document preUpdateDoc, Document updated) {
        written(updated.getShortId());
        if (whelk.getFeatures().isEnabled(INDEX_BLANK_WORKS)) {
            toRemove.addAll(minus(preUpdateDoc.getVirtualRecordIds(), updated.getVirtualRecordIds()));
        }
        if (!whelk.isSkipIndexDependers()) {
            if (Whelk.hasChangedMainEntityId(updated, preUpdateDoc)) {
                changedMainEntityIds.add(updated.getShortId());
            } else {
                linksChanged(updated, linksIfNotDeleted(preUpdateDoc), linksIfNotDeleted(updated));
            }
        }
    }

    public void removed(Document doc) {
        toRemove.add(doc.getShortId());
        if (whelk.getFeatures().isEnabled(INDEX_BLANK_WORKS)) {
            toRemove.addAll(doc.getVirtualRecordIds());
        }
        if (!whelk.isSkipIndexDependers()) {
            linksChanged(doc, doc.getExternalRefs(), Set.of());
        }
    }

    public void reindex(Collection<String> systemIds) {
        toReindex.addAll(systemIds);
    }

    public int flush() {
        try {
            submit(takePendingBatch());
            threadPool.awaitAll();
            try {
                whelk.elastic.refresh();
            } catch (Exception e) {
                log.warn("Could not refresh Elasticsearch indexes: {}", e.toString(), e);
            }

            toRemove.forEach(id -> whelk.elastic.remove(id));

            Set<String> ids = findAffected();
            log.info("Indexed {} written documents, indexing {} affected documents, removed {}",
                    writtenCount, ids.size(), toRemove.size());
            ids.stream().gather(Gatherers.windowFixed(BATCH_SIZE)).forEach(this::submit);
            return writtenCount + ids.size();
        } finally {
            threadPool.awaitAllAndShutdown();
        }
    }

    private void written(String systemId) {
        toRemove.remove(systemId);
        writtenCount++;
        pendingBatch.add(systemId);
        if (pendingBatch.size() >= BATCH_SIZE) {
            submit(takePendingBatch());
        }
    }

    private List<String> takePendingBatch() {
        List<String> batch = pendingBatch;
        pendingBatch = new ArrayList<>();
        return batch;
    }

    private void submit(List<String> batch) {
        if (!batch.isEmpty()) {
            threadPool.submit(() -> whelk.elastic.bulkIndexWithRetry(batch, whelk));
        }
    }

    // Wwhat Whelk.reindexAffected() would update
    private void linksChanged(Document document, Set<Link> preUpdateLinks, Set<Link> postUpdateLinks) {
        for (Link link : minus(postUpdateLinks, preUpdateLinks)) {
            addedLinks.computeIfAbsent(link.getIri(), iri -> new HashSet<>()).add(link.getRelation());
        }
        for (Link link : minus(preUpdateLinks, postUpdateLinks)) {
            removedLinkTargets.add(link.getIri());
        }
        // FIXME don't hardcode Item... (as in Whelk.reindexAffected())
        if ("Item".equals(document.getThingType()) || whelk.getStorage().isCardChangedOrNonexistent(document.getShortId())) {
            changedCardIris.addAll(document.getThingIdentifiers());
            changedCardIris.addAll(document.getRecordIdentifiers());
        }
    }

    private Set<String> findAffected() {
        Set<String> ids = new HashSet<>(toReindex);
        Map<String, String> targetIdByIri = new HashMap<>();
        try {
            Set<String> targetIris = new HashSet<>(addedLinks.keySet());
            targetIris.addAll(removedLinkTargets);
            for (String iri : targetIris) {
                String id = whelk.getStorage().getSystemIdByIri(iri);
                if (id != null && !id.isEmpty()) {
                    targetIdByIri.put(iri, id);
                }
            }
            ids.addAll(targetIdByIri.values());
        } catch (Exception e) {
            log.error("Error finding targets of changed links: {}", e.toString(), e);
        }

        try {
            List<String> addedLinkTargetIds = new ArrayList<>();
            for (String iri : addedLinks.keySet()) {
                if (targetIdByIri.containsKey(iri)) {
                    addedLinkTargetIds.add(targetIdByIri.get(iri));
                }
            }
            Map<String, Document> addedLinkTargets = whelk.bulkLoad(addedLinkTargetIds);
            for (var entry : addedLinks.entrySet()) {
                String id = targetIdByIri.get(entry.getKey());
                Document target = id != null ? addedLinkTargets.get(id) : null;
                if (target != null && entry.getValue().stream().anyMatch(r -> whelk.hasInReverseRelations(target, r))) {
                    for (Document dependent : whelk.integralReverseDependents(target)) {
                        ids.add(dependent.getShortId());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error finding documents with added links in their @reverse relations: {}", e.toString(), e);
        }

        changedCardIris.stream().gather(Gatherers.windowFixed(AFFECTED_IDS_BATCH_SIZE)).forEach(iris -> {
            try {
                for (String id : whelk.elastic.getAffectedIds(iris)) {
                    ids.add(id);
                }
            } catch (Exception e) {
                log.error("Error finding documents affected by changes to {}: {}", iris, e.toString(), e);
            }
        });

        for (String id : changedMainEntityIds) {
            try {
                ids.addAll(whelk.getStorage().getDependencies(id));
                ids.addAll(whelk.getStorage().getDependers(id));
            } catch (Exception e) {
                log.error("Error finding links of {}: {}", id, e.toString(), e);
            }
        }

        return ids;
    }

    // A deleted document links to nothing
    private static Set<Link> linksIfNotDeleted(Document doc) {
        return doc.getDeleted() ? Set.of() : doc.getExternalRefs();
    }

    private static <T> Set<T> minus(Collection<T> from, Collection<T> remove) {
        Set<T> result = new HashSet<>(from);
        result.removeAll(remove);
        return result;
    }
}

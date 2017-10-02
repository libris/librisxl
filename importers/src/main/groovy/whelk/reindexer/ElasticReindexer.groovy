package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.ThreadPool
import whelk.util.Tools

/**
 * Created by markus on 2015-12-10.
 */
@Log
class ElasticReindexer {

    static final int BATCH_SIZE = 5000
    Whelk whelk

    final static boolean useDocumentCache = true
    long startTime

    ElasticReindexer(Whelk w) {
        this.whelk = w
        this.whelk.loadCoreData()
    }

    synchronized void reindex(String suppliedCollection) {
        int counter = 0
        startTime = System.currentTimeMillis()
        List<String> collections = suppliedCollection ? [suppliedCollection] : whelk.storage.loadCollections()
        ThreadPool threadPool = new ThreadPool(Runtime.getRuntime().availableProcessors())
        collections.each { collection ->
            List<Document> documents = []
            for (document in whelk.storage.loadAll(collection)) {
                documents.add(document)
                Tools.printSpinner("Elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} seconds. Loaded $counter documents.", counter)
                counter++
                if (counter % BATCH_SIZE == 0) {
                    print("Elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} seconds. Loaded $counter documents. Bulk indexing ${documents.size()} documents ...")
                    threadPool.executeOnThread(new Batch(documents, collection), new BatchHandler())
                    documents = []
                }
            }
            if (documents.size() > 0) {
                whelk.elastic.bulkIndex(documents, collection, whelk, useDocumentCache)
            }
            log.info("Cache size: ${whelk.cacheSize()}, " +
                     "cache hits: ${whelk.cacheHits()}, " +
                     "stale cache reads: ${whelk.cacheStaleCount()} " +
                     "(after ${collection})")
        }
        threadPool.joinAll()
        println("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
    }

    private class Batch {
        Batch(List<Document> documents, String collection) {
            this.documents = documents
            this.collection = collection
        }
        List<Document> documents
        String collection
    }

    private class BatchHandler implements ThreadPool.Worker<Batch> {
        void doWork(Batch batch, int threadIndex) {
            long indexTime = System.currentTimeMillis()
            whelk.elastic.bulkIndex(batch.documents, batch.collection, whelk, useDocumentCache)
            println(" In ${(System.currentTimeMillis() - indexTime)} milliseconds.")
        }
    }
}

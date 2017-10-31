package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.Tools

/**
 * Created by markus on 2015-12-10.
 */
@Log
class ElasticReindexer {

    static final int BATCH_SIZE = 3000
    Whelk whelk

    ElasticReindexer(Whelk w) {
        this.whelk = w
        this.whelk.loadCoreData()
    }

    void reindex(String suppliedCollection) {
        boolean useDocumentCache = true
        int counter = 0
        long startTime = System.currentTimeMillis()
        List<String> collections = suppliedCollection ? [suppliedCollection] : whelk.storage.loadCollections()
        collections.each { collection ->
            List<Document> documents = []
            for (document in whelk.storage.loadAll(collection)) {
                documents.add(document)
                Tools.printSpinner("Elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} seconds. Loaded $counter documents.", counter)
                counter++
                if (counter % BATCH_SIZE == 0) {
                    long indexTime = System.currentTimeMillis()
                    print("Elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} seconds. Loaded $counter documents. Bulk indexing ${documents.size()} documents ...")
                    whelk.elastic.bulkIndex(documents, collection, whelk, useDocumentCache)
                    println(" In ${(System.currentTimeMillis() - indexTime)} milliseconds.")
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
        println("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
    }
}

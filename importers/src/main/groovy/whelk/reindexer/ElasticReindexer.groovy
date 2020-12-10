package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.ThreadPool

@Log
class ElasticReindexer {

    static final int BATCH_SIZE = 300
    static final int MAX_RETRIES = 5
    static final int RETRY_WAIT_MS = 3000

    Whelk whelk

    long startTime

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            void uncaughtException(Thread thread, Throwable throwable)
            {
                System.err.println("PANIC ABORT, unhandled exception:\n" + throwable.toString())
                throwable.printStackTrace()
                System.exit(-1)
            }
        })
    }

    ElasticReindexer(Whelk w) {
        this.whelk = w
    }

    /**
     * Reindex all changes since a point in time
     */
    void reindexFrom(long fromUnixTime) {
        try {
            int counter = 0
            for (collection in whelk.storage.loadCollections()) {
                boolean includeDeleted = true
                List<Document> documents = []
                for (document in whelk.storage.loadAll(collection, includeDeleted, new Date(fromUnixTime*1000))) {

                        documents.add(document)
                        counter++
                        if (counter % BATCH_SIZE == 0) {
                            double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                            println("Indexing $docsPerSec documents per second (running average since process start). Total count: $counter.")
                            bulkIndexWithRetries(documents, whelk)
                            documents = []
                        }
                }
                if (documents.size() > 0) {
                    bulkIndexWithRetries(documents, whelk)
                }
            }
        } catch (Throwable e) {
            println("Reindex failed with:\n" + e.toString() + "\ncallstack:\n" + e.printStackTrace())
        }
    }

    void reindex(String suppliedCollection) {
        try {
            int counter = 0
            startTime = System.currentTimeMillis()
            List<String> collections = suppliedCollection ? [suppliedCollection] : whelk.storage.loadCollections()
            ThreadPool threadPool = new ThreadPool(Runtime.getRuntime().availableProcessors() * 2)
            collections.each { collection ->
                List<Document> documents = []
                for (document in whelk.storage.loadAll(collection)) {
                    if ( ! document.getDeleted() ) {
                        documents.add(document)
                        counter++
                        if (counter % BATCH_SIZE == 0) {
                            double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                            println("Indexing $docsPerSec documents per second (running average since process start). Total count: $counter.")
                            threadPool.executeOnThread(new Batch(documents), new BatchHandler())
                            documents = []
                        }
                    }
                }
                if (documents.size() > 0) {
                    bulkIndexWithRetries(documents, whelk)
                }
            }
            threadPool.joinAll()
            println("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
            whelk.storage.logStats()
        } catch (Throwable e) {
            println("Reindex failed with:\n" + e.toString() + "\ncallstack:\n" + e.printStackTrace())
        }
    }

    private void bulkIndexWithRetries(List<Document> docs, Whelk whelk) {
        int retriesLeft = MAX_RETRIES

        Exception error
        while(error = tryBulkIndex(docs, whelk)) {
            if (retriesLeft-- > 0) {
                log.warn("Failed to index batch: [${error}], retrying after ${RETRY_WAIT_MS} ms")
                sleep()
            } else {
                log.warn("Failed to index batch: [${error}], max retries exceeded")
                throw error
            }
        }
    }

    private Exception tryBulkIndex(List<Document> docs, Whelk whelk) {
        try {
            whelk.elastic.bulkIndex(docs, whelk)
            return null
        }
        catch (Exception e) {
            return e
        }
    }

    private void sleep() {
        try {
            Thread.sleep(RETRY_WAIT_MS)
        }
        catch (InterruptedException e) {
            log.warn("Woke up early", e)
        }
    }

    private class Batch {
        List<Document> documents

        Batch(List<Document> documents) {
            this.documents = documents
        }
    }

    private class BatchHandler implements ThreadPool.Worker<Batch> {
        void doWork(Batch batch, int threadIndex) {
            bulkIndexWithRetries(batch.documents, whelk)
        }
    }
}

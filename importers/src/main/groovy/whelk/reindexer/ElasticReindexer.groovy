package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.BlockingThreadPool

import java.time.ZonedDateTime

@Log
class ElasticReindexer {

    static final int BATCH_SIZE = 300
    static final int MAX_RETRIES = 5
    static final int RETRY_WAIT_MS = 3000

    Whelk whelk

    long startTime

    // For example 1000 -> 1 means there's 1 thread whose last modified is 1000
    // For example 1000 -> 2 means there's 2 threads whose last modified is 1000
    HashMap<Long, Long> timestampsInFlight = new HashMap<>()
    long confirmedIndexedTo = 0

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

    private synchronized void stillIndexing(long timeStamp) {
        Long old = timestampsInFlight.get(timeStamp)
        if (old == null)
            timestampsInFlight.put(timeStamp, 1)
        else {
            timestampsInFlight.put(timeStamp, old + 1)
        }
    }

    private synchronized void noLongerIndexing(long timeStamp) {
        Long old = timestampsInFlight.get(timeStamp)
        assert(old != null)
        if (old > 1)
            timestampsInFlight.put(timeStamp, old - 1)
        else if (old == 1) {
            long lowestInFlight = timestampsInFlight.keySet().sort().first()
            if (timeStamp == lowestInFlight)
                confirmedIndexedTo = lowestInFlight
            timestampsInFlight.remove(timeStamp)
        }
    }

    /**
     * Reindex all changes since a point in time
     */

    void reindexFrom(long fromUnixTime) {
        BlockingThreadPool.SimplePool threadPool
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
                        println("Indexing $docsPerSec documents per second (running average since process start). Total count: $counter. Confirmed indexed up to UNIX-time: ${confirmedIndexedTo-1}")
                        def batch = documents
                        threadPool.submit({
                            long indexedToUnixTime = ZonedDateTime.parse(document.getModified()).toInstant().getEpochSecond()
                            stillIndexing(indexedToUnixTime)
                            bulkIndexWithRetries(batch, whelk)
                            noLongerIndexing(indexedToUnixTime)
                        })
                        documents = []
                    }
                }
                if (documents.size() > 0) {
                    bulkIndexWithRetries(documents, whelk)
                }
            }
        } catch (Throwable e) {
            println("Reindex failed with:\n" + e.toString() + "\ncallstack:\n" + e.printStackTrace())
        } finally {
            threadPool.awaitAllAndShutdown()
        }
    }

    void reindex(String suppliedCollection, int numberOfThreads) {
        BlockingThreadPool.SimplePool threadPool
        try {
            int counter = 0
            startTime = System.currentTimeMillis()
            List<String> collections = suppliedCollection ? [suppliedCollection] : whelk.storage.loadCollections()
            threadPool = BlockingThreadPool.simplePool(numberOfThreads)
            collections.each { collection ->
                List<Document> documents = []
                for (document in whelk.storage.loadAll(collection)) {
                    if ( ! document.getDeleted() ) {
                        documents.add(document)
                        counter++
                        if (counter % BATCH_SIZE == 0) {
                            double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                            println("Indexing $docsPerSec documents per second (running average since process start). Total count: $counter. Confirmed indexed up to UNIX-time: ${confirmedIndexedTo-1}")
                            def batch = documents
                            threadPool.submit({
                                long indexedToUnixTime = ZonedDateTime.parse(document.getModified()).toInstant().getEpochSecond()
                                stillIndexing(indexedToUnixTime)
                                bulkIndexWithRetries(batch, whelk)
                                noLongerIndexing(indexedToUnixTime)
                            })
                            documents = []
                        }
                    }
                }
                if (documents.size() > 0) {
                    bulkIndexWithRetries(documents, whelk)
                }
            }
            println("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
            println("New number of mappings/fields in ES: ${whelk.elastic.getFieldCount()}")
            whelk.storage.logStats()
        } catch (Throwable e) {
            println("Reindex failed with:\n" + e.toString() + "\ncallstack:\n" + e.printStackTrace())
        } finally {
            threadPool.awaitAllAndShutdown()
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
}

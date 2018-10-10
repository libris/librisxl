package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.ThreadPool
import whelk.util.Tools

@Log
class ElasticReindexer {

    static final int BATCH_SIZE = 1000
    Whelk whelk

    long startTime

    // Abort on unhandled exceptions, including those on worker threads.
    static
    {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable)
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
                            whelk.elastic.bulkIndex(documents, collection, whelk)
                            documents = []
                        }
                }
                if (documents.size() > 0) {
                    whelk.elastic.bulkIndex(documents, collection, whelk)
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
            ThreadPool threadPool = new ThreadPool(Runtime.getRuntime().availableProcessors() * 4)
            collections.each { collection ->
                List<Document> documents = []
                for (document in whelk.storage.loadAll(collection)) {
                    if ( ! document.getDeleted() ) {
                        documents.add(document)
                        counter++
                        if (counter % BATCH_SIZE == 0) {
                            double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                            println("Indexing $docsPerSec documents per second (running average since process start). Total count: $counter.")
                            threadPool.executeOnThread(new Batch(documents, collection), new BatchHandler())
                            documents = []
                        }
                    }
                }
                if (documents.size() > 0) {
                    whelk.elastic.bulkIndex(documents, collection, whelk)
                }
            }
            threadPool.joinAll()
            println("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
        } catch (Throwable e) {
            println("Reindex failed with:\n" + e.toString() + "\ncallstack:\n" + e.printStackTrace())
        }
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
            whelk.elastic.bulkIndex(batch.documents, batch.collection, whelk)
        }
    }
}

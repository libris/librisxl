package whelk.reindexer

import whelk.Document
import whelk.Whelk
import whelk.util.ThreadPool
import groovy.util.logging.Log4j2 as Log

import java.time.Instant

@Log
class CardRefresher {
    static final int BATCH_SIZE = 1000

    Whelk whelk
    Instant timestamp = Instant.now()

    CardRefresher(Whelk whelk) {
        this.whelk = whelk
    }

    void refresh(String collection = null) {
        try {
            int counter = 0
            long startTime = System.currentTimeMillis()

            ThreadPool threadPool = new ThreadPool(whelk.storage.getPoolSize())

            List<Document> documents = new ArrayList<>(BATCH_SIZE)
            for (document in whelk.storage.loadAll(collection)) {
                documents.add(document)
                counter++
                if (counter % BATCH_SIZE == 0) {
                    double docsPerSec = ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                    log.info("Refreshing $docsPerSec cards per second (running average since process start). Total count: $counter.")
                    threadPool.executeOnThread(documents, new BatchHandler())
                    documents = []
                }
            }
            refreshCards(documents)
            threadPool.joinAll()
            log.info("Done! $counter documents reindexed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
        } catch (Exception e) {
            log.error("Refresh cards failed: $e", e)
        }
    }

    private void refreshCards(List<Document> documents) {
        for (Document document : documents) {
            try {
                whelk.storage.refreshCardData(document, timestamp)
            }
            catch (Exception e) {
                log.error("Error refreshing card for ${document.shortId}: $e", e)
            }
        }
    }

    private class BatchHandler implements ThreadPool.Worker<List<Document>> {
        void doWork(List<Document> batch, int threadIndex) {
            refreshCards(batch)
        }
    }
}

package whelk.reindexer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.BlockingThreadPool

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
            long counter = 0
            long startTime = System.currentTimeMillis()

            BlockingThreadPool.SimplePool threadPool = BlockingThreadPool.simplePool(whelk.storage.getPoolSize())

            List<Document> documents = new ArrayList<>(BATCH_SIZE)
            for (document in whelk.storage.loadAll(collection)) {
                documents.add(document)
                counter++
                if (counter % BATCH_SIZE == 0) {
                    int docsPerSec = (int) ((double) counter) / ((double) ((System.currentTimeMillis() - startTime) / 1000))
                    log.info("Processing $docsPerSec docs per second (running average since process start). Total count: $counter.")
                    threadPool.submit(documents, this::refreshCards)
                    documents = new ArrayList<>(BATCH_SIZE)
                }
            }
            refreshCards(documents)
            threadPool.awaitAllAndShutdown()

            log.info("Done! $counter documents processed in ${(System.currentTimeMillis() - startTime) / 1000} seconds.")
            whelk.storage.logStats()
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
}

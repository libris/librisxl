package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document

@Log
class IndexingPrawn extends GetPrawn implements Prawn, WhelkAware {

    private final BlockingQueue<Document> queue = new LinkedBlockingQueue<Document>()
    Whelk whelk

    // Wait for batch size until performing operation
    int BATCH_SIZE = 1000
    // Or until timeout
    int TIMEOUT = 10 * 1000// in seconds

    boolean active = true

    IndexingPrawn(Map settings) {
        this.BATCH_SIZE = settings.get("batchSize", BATCH_SIZE).intValue()
        this.TIMEOUT = settings.get("timeout", TIMEOUT).intValue()
    }

    void run() {
        active = true
        Map docs = [:]
        long timestamp = System.nanoTime()
        try {
            while (active) {
                def doc = queue.take()
                log.debug("Got ${doc.identifier} from queue")
                docs.put(doc.identifier, doc)
                long diff = ((System.nanoTime() - timestamp) / 1000000)
                if (docs.size() >= BATCH_SIZE
                        || (!docs.empty &&  diff > TIMEOUT)) {
                    log.debug("$diff time elapsed since last run. We're doing this.")
                        reindex(docs.values() as List)
                        docs = [:]
                        timestamp = System.nanoTime()
                        }
            }
        } catch (InterruptedException ie) {
            deactivate()
        }
    }

    void reindex(final List<Document> docs) {
        if (docs.size() > 0) {
            log.debug("Re-adding ${docs.size()} documents to index for whelk ${whelk.id}")
            whelk.addToIndex(docs)
        }
    }

    void deactivate() {
        log.info("Shutting down prawn ${this.id}")
        this.active = false
    }

    BlockingQueue getQueue() {
        return queue
    }

}

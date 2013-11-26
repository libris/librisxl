package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document

@Log
class IndexingPrawn extends BasicPlugin implements Prawn, WhelkAware {

    private final BlockingQueue<Document> queue = new LinkedBlockingQueue<Document>()
    Whelk whelk

    String id = "prawn"

    // Wait for batch size until performing operation
    int BATCH_SIZE = 1000
    // Or until timeout
    int TIMEOUT = 10 * 1000// in seconds

    IndexingPrawn(Map settings) {
        this.BATCH_SIZE = settings.get("batchSize", BATCH_SIZE).intValue()
        this.TIMEOUT = settings.get("timeout", TIMEOUT).intValue()
    }

    void run() {
        Map docs = [:]
        long timestamp = System.nanoTime()
        while (true) {
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
    }

    void reindex(final List<Document> docs) {
        if (docs.size() > 0) {
            log.debug("Re-adding ${docs.size()} documents to index for whelk ${whelk.id}")
            whelk.addToIndex(docs)
        }
    }

    BlockingQueue getQueue() {
        return queue
    }

}

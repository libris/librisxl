package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document

@Log
class IndexingPrawn extends BasicPlugin implements Prawn, WhelkAware {

    private final BlockingQueue<String> queue
    Whelk whelk

    String id = "prawn"

    // Wait for batch size until performing operation
    final int BATCH_SIZE = 1000
    // Or until timeout
    final int TIMEOUT = 10 * 1000// in seconds

    IndexingPrawn() {
        queue = new LinkedBlockingQueue<String>()
    }

    void run() {
        Set uris = new HashSet<String>()
        long startTime = System.nanoTime()
        while (true) {
            def uri = queue.take()
            log.debug("Got $uri from queue")
            uris.add(uri)
            if (uris.size() >= BATCH_SIZE
                    || (uris.size() > 0 && ((System.nanoTime() - startTime) / 1000000) > TIMEOUT)) {
                log.debug("Running reindex batch.")
                reindex(uris)
                uris = new HashSet<String>()
                startTime = System.nanoTime()
            }
        }
    }

    void reindex(final Set<String> uris) {
        List<Document> docs = uris.collect { whelk.get(new URI(it)) }
        log.debug("Re-adding ${docs.size()} documents to index for whelk ${whelk.id}")
        whelk.addToIndex(docs)
    }

    BlockingQueue getQueue() {
        return queue
    }

}

package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document

@Log
class IndexingPrawn extends GetPrawn implements Prawn, WhelkAware {

    private final BlockingQueue<Document> queue = new LinkedBlockingQueue<Document>()
    Whelk whelk
    String requiredContentType = "application/ld+json"

    // Wait for batch size until performing operation
    int BATCH_SIZE = 1000
    // Or until timeout
    int TIMEOUT = 10 * 1000// in seconds

    boolean active = true

    def TYPES_TO_EXTRACT = ["Person"]

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
                if (doc.contentType == requiredContentType) {
                    log.debug("Got ${doc.identifier} from queue")
                    docs.put(doc.identifier, doc)
                    long diff = ((System.nanoTime() - timestamp) / 1000000)
                    log.debug("diff is $diff (timeout: $TIMEOUT)")
                    if (docs.size() >= BATCH_SIZE || (!docs.empty &&  diff > TIMEOUT)) {
                        log.debug("$diff time elapsed since last run. We're doing this.")
                        extractAndReindex(docs.values() as List)
                        docs = [:]
                        timestamp = System.nanoTime()
                    } else if (log.isDebugEnabled()) {
                        if (docs.size() < BATCH_SIZE) {
                            log.debug("Not executing queue, because we only have ${docs.size()} entries. (Need $BATCH_SIZE to override timeout.)")
                        }
                        if (diff < TIMEOUT) {
                            log.debug("Not executing queue, because not enough time has elapsed.")
                        }
                    }
                }
            }
        } catch (InterruptedException ie) {
            deactivate()
        }
    }

    void extractAndReindex(final List<Document> docs) {
        def extrDocs = []
        for (doc in docs) {
            log.info("Looking at structure of ${doc.identifier}")
            def map = findMaps(doc.dataAsMap)
            extrDocs << doc.withData(map)
        }
        if (extrDocs.size() > 0) {
            log.debug("Re-adding ${extrDocs.size()} documents to index for whelk ${whelk.id}")
            whelk.addToIndex(extrDocs)
        }
    }

    Map findMaps(map) {
        map.each { key, value ->
            if (value instanceof Map) {
                def t = value.get("@type")
                def i = value.get("@id")
                if (TYPES_TO_EXTRACT.contains(t) && i) {
                    // TODO: clean out old data from node
                    value.putAll(loadResource(i))
                }
                findMaps(value)
            } else if (value instanceof List) {
                int idx = 0
                for (v in value) {
                    if (v instanceof Map) {
                        def t = v.get("@type")
                        def i = v.get("@id")
                        if (TYPES_TO_EXTRACT.contains(t) && i) {
                            // TODO: clean out old data from node
                            v.putAll(loadResource(i))
                        }
                        findMaps(v)
                    }
                    idx++
                }
            }
        }
        return map
    }

    Map loadResource(String id) {
        def entry = whelk.get(new URI(id.replace("/resource", "")), null, [], true)
        if (entry) {
            def about = entry.dataAsMap['about']
            log.info("about: $about")
            return about
        }
        return [:]
    }

    void deactivate() {
        log.info("Shutting down prawn ${this.id}")
        this.active = false
    }

    BlockingQueue getQueue() {
        return queue
    }
}

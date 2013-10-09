package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

@Log
class InMemoryStorage extends BasicPlugin implements Storage {
    def storage = new HashMap<URI, Document>()
    String id = "inmemorystorage"
    String requiredContentType
    Whelk whelk
    boolean enabled = true
    boolean isEnabled() { return enabled }
    void enable() {this.enabled = true}
    void disable() {this.enabled = false}

    boolean store(Document d, String wp) {
        println "Extra allt ..."
        log.debug("Saving document $d.identifier to memory.")
        storage.put(d.identifier, d)
        return true
    }

    void store(Iterable<Document> docs, String wp) {
        for (Document d : docs) {
            storage.put(d.identifier, d)
        }
    }

    Iterable<Document> getAll(String wp) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document get(URI uri, String wp) {
        return storage.get(uri)
    }

    void delete(URI uri, String wp) {
        storage.remove(uri)
    }
}

package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

@Log
class InMemoryStorage implements Storage {
    def storage = new HashMap<URI, Document>()
    String id = "inmemorystorage"
    Whelk whelk
    boolean enabled = true
    boolean isEnabled() { return enabled }
    void enable() {this.enabled = true}
    void disable() {this.enabled = false}

    void store(Document d) {
        println "Extra allt ..."
        log.debug("Saving document $d.identifier to memory.")
        storage.put(d.identifier, d)
    }

    void store(Iterable<Document> docs) {
        for (Document d : docs) {
            storage.put(d.identifier, d)
        }
    }

    Document get(URI uri) {
        return storage.get(uri)
    }

    void delete(URI uri) {
        storage.remove(uri)
    }

    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}

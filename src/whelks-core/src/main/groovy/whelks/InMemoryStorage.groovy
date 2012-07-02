package se.kb.libris.whelks.component

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

class InMemoryStorage implements Storage {
    def storage = {}
    String id = "inmemorystorage"
    Whelk whelk
    boolean enabled = true
    boolean isEnabled() { return enabled }
    void enable() {this.enabled = true}
    void disable() {this.enabled = false}

    void store(Document d) {
        storage[d.identifier] = d
    }

    Document get(URI uri) {
        return storage[uri]
    }

    void delete(URI uri) {
        storage.remove(uri)
    }

    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}

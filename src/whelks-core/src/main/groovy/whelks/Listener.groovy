package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*

class Listener implements Plugin {

    ListeningWhelk whelk
    Whelk notifier

    String id = "whelkListener"

    Listener(n) {
        this.notifier = n
    }

    void setWhelk(Whelk w) {
        if (w instanceof ListeningWhelk) {
            this.whelk = w
            this.notifier.addPluginIfNotExists(new Notifier(this.whelk))
            id = id + ", listening to $notifier.prefix"
            id = id + " for $w.prefix"
        } else {
            throw new WhelkRuntimeException("Listener designed to work only with ListeningWhelk.")
        }
    }
}

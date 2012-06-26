package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*

class Listener implements Plugin {

    def whelk
    def notifier

    String id = "whelkListener"
    
    Listener(n) {
        this.notifier = n
    }

    void setWhelk(Whelk w) {
        this.whelk = w
        this.notifier.addPluginIfNotExists(new Notifier(this.whelk))
        id = id + ", listening to $notifier.prefix"
        id = id + " for $w.prefix"
    }
}

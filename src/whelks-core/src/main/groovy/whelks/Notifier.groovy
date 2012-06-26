package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*

class Notifier extends AbstractTrigger {

    Listener listener

    String id = "notificationTrigger"

    Notifier(Listener l) {
        this.listener = l
    }

    @Override
    void afterStore(Document doc) {
        listener.notify(this, doc.timestamp)
    }

    @Override
    boolean equals(Object o) {
        if (o && o instanceof Notifier) {
            Notifier n = (Notifier)n
            if (listener && n.listener && listener.prefix == n.listener.prefix) {
                return true
            }
        }
        return false
    }

}

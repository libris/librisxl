package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*

class Notifier extends AbstractTrigger {

    ListeningWhelk listener

    String id = "notificationTrigger"

    Notifier(ListeningWhelk w) {
        this.listener = w
    }

    @Override
    void afterStore(Document doc) {
        listener.notify(this, doc.identifier)
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

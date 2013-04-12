package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class Notifier extends AbstractTrigger {

    Listener listener

    String id = "notificationTrigger"
    boolean enabled = true

    Notifier(Listener l) {
        this.listener = l
    }

    void enable() {
        log.debug("Enabling notifier.")
        this.enabled = true
    }
    void disable() {
        log.debug("Disabling notifier.")
        this.enabled = false
    }

    @Override
    void afterStore(Document doc) {
        listener.notify(doc)
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

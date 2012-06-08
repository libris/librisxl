package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class NotificationTrigger implements Trigger {

    boolean enabled = true
    String id = "notificationTrigger"
    Whelk whelk

    public void setWhelk(Whelk w) {this.whelk = w}

    public void afterStore(Document d) {
        log.debug("Trigger for whelk ${this.whelk} notifying listeners ...")
        this.whelk.manager.notifyListeners(d.identifier)
    }

    public void afterGet(Document d){}
    public void afterDelete(URI uri){}

    public void beforeStore(Document d) {}
    public void beforeGet(Document d){}
    public void beforeDelete(URI uri){}


    public void enable() {this.enabled = true}
    public void disable() {this.enabled = false}
}

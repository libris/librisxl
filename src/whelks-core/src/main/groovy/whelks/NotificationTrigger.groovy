package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class NotificationTrigger implements Trigger {

    boolean enabled = true
    String id = "notificationTrigger"

    public void afterStore(Whelk whelk, Document d) {
        log.debug("Trigger for whelk ${whelk} notifying listeners ...")
        whelk.manager.notifyListeners(d.identifier)
    }

    public void afterGet(Whelk whelk, Document d){}
    public void afterDelete(Whelk whelk, URI uri){}

    public void beforeStore(Whelk whelk, Document d) {}
    public void beforeGet(Whelk whelk, Document d){}
    public void beforeDelete(Whelk whelk, URI uri){}


    public void enable() {this.enabled = true}
    public void disable() {this.enabled = false}
}

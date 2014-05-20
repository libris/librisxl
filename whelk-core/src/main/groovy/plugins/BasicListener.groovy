package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import se.kb.libris.whelks.*

@Log
class BasicListener extends BasicPlugin implements Listener {

    private static BasicListener instance = null

    Map registry
    Whelk whelk

    private BasicListener() {}
    private Map queues = [:]

    public static BasicListener getInstance() {
        if (instance == null) {
            instance = new BasicListener()
        }
        return instance
    }

    @Override
    void init(String id) {
        super.init(id)
        // Setup listener queues for all listening components.
        registry.each { sender, receiverlist ->
            for (receiver in receiverlist) {
                log.debug("Creating queue for $receiver")
                queues.put(receiver, new LinkedBlockingQueue<ListenerEvent>())
            }
        }
    }

    void registerUpdate(String componentId, Object value, boolean force = false) {
        log.trace("Listeners for $componentId: " + registry.get(componentId, []))
        for (listener in registry.get(componentId, [])) {
            log.debug("Listener \"$listener\" registering update $value from $componentId")
            queues.get(listener).offer(new ListenerEvent(componentId, value, force))
            if (queues.get(listener).size() > 0) {
                log.debug("Queue size for ${listener}: ${queues.get(listener).size()}")
            }
        }
    }

    /**
     * Called by listening component.
     * @param componentId the id of the calling component.
     * @return ListenerEvent the next in queue for componentId.
     */
    ListenerEvent nextUpdate(String componentId) {
        return queues.get(componentId)?.take()
    }


    boolean hasQueue(String componentId) {
        return queues.containsKey(componentId)
    }

    void disable(String componentId) {
    }
}

class ListenerEvent {
    String senderId
    Object payload
    boolean force = false

    ListenerEvent(String s, Object p, boolean f=false) {
        this.senderId = s
        this.payload = p
        this.force = f
    }
}

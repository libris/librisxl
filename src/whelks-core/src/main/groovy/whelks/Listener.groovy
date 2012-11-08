package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

@Log
class Listener extends BasicPlugin implements WhelkAware {

    Whelk homewhelk
    Whelk otherwhelk
    FormatConverter converter

    List documents = Collections.synchronizedList(new LinkedList())

    final int DEFAULT_NUMBER_OF_HANDLERS = 50
    final int STATE_SAVE_INTERVAL = 10000
    final int CHECK_AGAIN_DELAY = 500
    int numberOfHandlers = DEFAULT_NUMBER_OF_HANDLERS

    def pool

    String id = "whelkListener"
    boolean enabled = true
    boolean isEnabled() {return enabled}
    void disable() {this.enabled = false}
    Date lastUpdate = null

    Listener(Whelk n) {
        this.otherwhelk = n
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
        pool = java.util.concurrent.Executors.newCachedThreadPool()
    }

    /*
    Listener(Whelk n, int nrOfHandlers) {
        this.otherwhelk = n
        this.numberOfHandlers = nrOfHandlers
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
    }
    */

    void setWhelk(Whelk w) {
        this.homewhelk = w
        id = id + " for $w.prefix"
        try {
            lastUpdate = new Date(new Long(new File("/tmp/whelk_listener_state_${this.homewhelk.prefix}-${this.otherwhelk.prefix}").text))
        } catch (Exception e) {
            log.info("Didn't find a valid state-file.")
        }
        if (!lastUpdate) {
            log.debug("Didn't find a last updated time. Setting epoch.")
            lastUpdate = new Date(0L)
        }
        /*
        log.info("Starting $numberOfHandlers handlers.")
        for (int i = 0; i < this.numberOfHandlers; i++) {
            new Thread(new UpdateHandler()).start()
        }
        */
        Thread.start {
            Date lastSavedUpdate = lastUpdate
            while (true) {
                sleep(STATE_SAVE_INTERVAL)
                if (lastUpdate && lastUpdate != lastSavedUpdate) {
                    log.debug("Saving state.")
                    new File("/tmp/whelk_listener_state_${this.homewhelk.prefix}-${this.otherwhelk.prefix}").text = "" + lastUpdate.time
                    lastSavedUpdate = lastUpdate
                }
            }
        }
        Thread.start {
            def updates = otherwhelk.log(lastUpdate)
            def iter = updates.iterator()
            if (iter.hasNext()) {
                log.info("Found updates. Populating documents-list.")
                iter.each {
                    notify(it)
                }
            }
        }
    }

    void notify(Document doc) {
        log.trace "Whelk $homewhelk.prefix notified of change in $doc"
        /*
        if (!documents.contains(doc)) {
            documents.push(doc)
        }
        */
        pool.submit(new Runnable() {
            public void run() {
                log.debug("Pushing ${doc.identifier} to $homewhelk")
                homewhelk.store(doc)
            }
        })
    }

    void notify(Date timestamp) {
        log.trace "Whelk $homewhelk.prefix notified of change since $timestamp"
        boolean updatesReceived = false
        while (!updatesReceived) {
            def updates = otherwhelk.log(timestamp)
            def iter = updates.iterator()
            if (iter.hasNext()) {
                log.info("Found updates. Populating documents-list.")
                iter.each {
                    notify(it)
                }
                updatesReceived = true
            } else {
                log.debug("Got no updates, despite notification. Waiting a while ...")
                sleep(CHECK_AGAIN_DELAY)
            }
        }
    }
    /*
    @Synchronized
    Document nextDocument() {
        try {
            Document d = documents.pop()
            log.debug("nextDocument returning ${d.identifier}. List contains " + documents.size() + " items.")
            return d
        } catch (NoSuchElementException nsee) {
            return null
        }
    }
    */

    void enable() {
        this.enabled = true
        if (lastUpdate) {
            notify(lastUpdate)
        }
    }

    /*
    @Log
    class UpdateHandler implements Runnable {
        def converter

        UpdateHandler() {
        }

        void run() {
            while (true) {
                def doc = nextDocument()
                if (doc) {
                    log.debug("Next is $doc.identifier")
                    homewhelk.store(doc)
                    lastUpdate = doc.timestampAsDate
                    //convert(otherwhelk.get(uri))
                }
                sleep(CHECK_AGAIN_DELAY)
            }
            log.error("Thread is exiting ...")
        }
    }
    */
}

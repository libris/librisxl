package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import java.util.concurrent.*
import java.util.concurrent.atomic.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.imports.*

@Log
class Listener extends BasicPlugin implements WhelkAware {

    Whelk homewhelk
    Whelk otherwhelk
    FormatConverter converter

    List documents = Collections.synchronizedList(new LinkedList())

    final int DEFAULT_NUMBER_OF_HANDLERS = 2
    final int MAX_NUMBER_OF_HANDLERS = 10
    final int STATE_SAVE_INTERVAL = 10000
    final int CHECK_AGAIN_DELAY = 500

    def pool

    String id = "whelkListener"
    boolean enabled = true
    boolean isEnabled() {return enabled}
    void disable() {this.enabled = false}
    final AtomicLong lastUpdate = new AtomicLong()

    Listener(Whelk n) {
        this.otherwhelk = n
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
        //pool = java.util.concurrent.Executors.newCachedThreadPool()
        pool = newScalingThreadPoolExecutor(DEFAULT_NUMBER_OF_HANDLERS, MAX_NUMBER_OF_HANDLERS, 60)
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
            lastUpdate.set(new File("/tmp/whelk_listener_state_${this.homewhelk.prefix}-${this.otherwhelk.prefix}").text.longValue())
        } catch (Exception e) {
            log.info("Didn't find a valid state-file.")
        }
        if (!lastUpdate) {
            log.debug("Didn't find a last updated time. Setting epoch.")
            lastUpdate.set(0L)
        }
        /*
        log.info("Starting $numberOfHandlers handlers.")
        for (int i = 0; i < this.numberOfHandlers; i++) {
            new Thread(new UpdateHandler()).start()
        }
        */
        Thread.start {
            long lastSavedUpdate = lastUpdate.get()
            while (true) {
                sleep(STATE_SAVE_INTERVAL)
                if (lastUpdate && lastUpdate.get() != lastSavedUpdate) {
                    log.debug("Saving state.")
                    new File("/tmp/whelk_listener_state_${this.homewhelk.prefix}-${this.otherwhelk.prefix}").text = "" + lastUpdate.get()
                    lastSavedUpdate = lastUpdate.get()
                }
            }
        }
        Thread.start {
            def updates = otherwhelk.log(new Date(lastUpdate.get()))
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
        pool.execute(new Runnable() {
            public void run() {
                log.debug("Pushing ${doc.identifier} to $homewhelk (${homewhelk.prefix})")
                lastUpdate.set(doc.getTimestamp())
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

    void enable() {
        this.enabled = true
        if (lastUpdate) {
            notify(lastUpdate)
        }
    }

    public ExecutorService newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        ScalingQueue queue = new ScalingQueue()
        ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }
}

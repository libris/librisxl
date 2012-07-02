package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.Exchanger

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*

@Log
class Listener implements WhelkAware {

    Whelk homewhelk
    Whelk otherwhelk
    FormatConverter converter

    LinkedList notifications = new LinkedList<Object>()

    final int DEFAULT_NUMBER_OF_FETCHERS = 1
    final int STATE_SAVE_INTERVAL = 10000
    String id = "whelkListener"
    boolean enabled = true
    boolean isEnabled() {return enabled}
    void disable() {this.enabled = false}
    Date lastUpdate = null


    Listener(Whelk n, FormatConverter conv, int numberOfFetchers) {
        startup(n, conv, numberOfFetchers)
    }

    Listener(Whelk n, FormatConverter conv) {
        startup(n, conv, DEFAULT_NUMBER_OF_FETCHERS)
    }

    private void startup(Whelk n, FormatConverter conv, int numberOfFetchers) {
        this.otherwhelk = n
        this.converter = conv
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
        new Thread(new UpdateFetcher()).start()
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
    }

    void setWhelk(Whelk w) {
        this.homewhelk = w
        id = id + " for $w.prefix"
        try {
            lastUpdate = new Date(new Long(new File("/tmp/whelk_listener_state_${this.homewhelk.prefix}-${this.otherwhelk.prefix}").text))
        } catch (Exception e) {
            log.info("Failed to read statefile. Ignoring.")
        }
        if (!lastUpdate) {
            log.debug("Didn't find a last updated time. Setting epoch.")
            lastUpdate = new Date(0L)
        }
        notify(lastUpdate)
    }

    void notify(URI identifier) {
        log.debug "Whelk $homewhelk.prefix notified of change in $identifier"
        notifications.push(identifier)
    }

    void notify(Date timestamp) {
        log.debug "Whelk $homewhelk.prefix notified of change since $timestamp"
        notifications.push(timestamp)
    }

    void enable() {
        this.enabled = true
        if (lastUpdate) {
            notify(lastUpdate)
        }
    }

    @Log
    class UpdateFetcher implements Runnable {

        final int CHECK_AGAIN_DELAY = 500
        File logfile = new File("listener.log")
        FileWriter fw
        BufferedWriter bw

        UpdateFetcher() {
            log.debug("Starting fetcher thread ...")
            fw = new FileWriter(logfile, true)
            bw = new BufferedWriter(fw)
        }

        void listenerLog(String message) {
            bw.write(message+"\n")
            bw.flush()
        }

        void run() {
            while (notifications != null) {
                try {
                    while (notifications.size() > 0) {
                        def next = notifications.pop()
                        listenerLog("Working off notification list with " + notifications.size() + " items. Next is $next.")
                        log.debug("Next object off list: $next")
                        if (next instanceof Date) {
                            def updates = otherwhelk.log(next)
                            def iter = updates.iterator()
                            if (iter.hasNext()) {
                                log.debug("Found updates.")
                                iter.each {
                                    convert(otherwhelk.get(it.identifier))
                                }
                            } else {
                                log.debug("Got no updates, despite notification. Waiting a while ...")
                                Thread.sleep(CHECK_AGAIN_DELAY)
                            }
                        } else if (next instanceof URI) {
                            log.debug("Notified of document change in $next")
                            convert(otherwhelk.get(next))
                        }
                    }
                } catch (Throwable t) {
                    log.error("Got exception. Keep cool and carry on ...", t)
                }
                // Sleep for one second after running.
                Thread.sleep(1000)
            }
            log.error("Thread is exiting ...")
        }

        void convert(Document doc) {
            log.debug("Converting document ...")
            Document convertedDocument = converter.convert(doc)
            lastUpdate = doc.timestampAsDate
            log.debug("Done ...")
            if (convertedDocument) {
                log.debug("New document created/converted with identifier ${convertedDocument.identifier}")
                homewhelk.store(convertedDocument)
            }
        }

    }
}

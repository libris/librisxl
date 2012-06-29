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

    Exchanger<LinkedList<Object>> exchanger = new Exchanger<LinkedList<Object>>()
    LinkedList notifications = new LinkedList<Object>()

    String id = "whelkListener"

    Listener(Whelk n, FormatConverter conv) {
        this.otherwhelk = n
        this.converter = conv
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        id = id + ", listening to $otherwhelk.prefix"
        new Thread(new UpdateFetcher()).start()
    }

    void setWhelk(Whelk w) {
        this.homewhelk = w
        id = id + " for $w.prefix"
    }

    void notify(URI identifier) {
        log.debug "Whelk $homewhelk.prefix notified of change in $identifier";
        notifications.push(identifier)
        exchanger.exchange(notifications) 
        log.debug("switched ...")
    }

    void notify(Date timestamp) {
        log.debug "Whelk $homewhelk.prefix notified of change since $timestamp";
        notifications.push(timestamp)
        exchanger.exchange(notifications) 
        log.debug("switched ...")
    }

    @Log
    class UpdateFetcher implements Runnable {

        static final int CHECK_AGAIN_DELAY = 500

        UpdateFetcher() {
            log.debug("Starting fetcher thread ...")
        }

        void run() {
            log.debug("Calling log on whelk ${otherwhelk.prefix}")
            LinkedList<Date> worklist = notifications;
            while (worklist != null) {
                try {
                    log.debug("Starting work on worklist ...")
                    while (worklist.size() > 0) {
                        def next = worklist.peek()
                        log.debug("Next object off list: $next")
                        if (next instanceof Date) {
                            def updates = otherwhelk.log(next)
                            if (updates.size() > 0) {
                                log.debug("Found updates.")
                                updates.each {
                                    worklist.removeFirstOccurrence(it.timestamp)
                                    convert(otherwhelk.get(it.identifier));
                                }
                            } else {
                                log.debug("Got no updates, despite notification. Waiting a while ...");
                                Thread.sleep(CHECK_AGAIN_DELAY)
                            }
                        } else if (next instanceof URI) {
                            log.debug("Notified of document change in $next");
                            worklist.removeFirstOccurrence(next)
                            convert(otherwhelk.get(next));
                        } else {
                            worklist.pop()
                        }
                    }
                    log.debug("Done with worklist. Switching ...")
                    exchanger.exchange(worklist)
                } catch (Throwable t) {
                    log.error("Got exception. Keep cool and carry on ...", t)
                }
            }
            log.error("Thread is exiting ...")
        }

        void convert(Document doc) {
            log.debug("Converting document ...")
            Document convertedDocument = converter.convert(doc)
            log.debug("Done ...")
            if (convertedDocument) {
                log.debug("New document created/converted with identifier ${convertedDocument.identifier}")
                    homewhelk.store(convertedDocument)
            }
        }

    }
}

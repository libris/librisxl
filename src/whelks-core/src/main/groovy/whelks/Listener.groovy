package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*

@Log
class Listener implements WhelkAware {

    Whelk homewhelk
    Whelk otherwhelk
    FormatConverter converter

    List identifiers = Collections.synchronizedList(new LinkedList())

    final int DEFAULT_NUMBER_OF_HANDLERS = 1
    final int STATE_SAVE_INTERVAL = 10000
    final int CHECK_AGAIN_DELAY = 500
    int numberOfHandlers = DEFAULT_NUMBER_OF_HANDLERS

    String id = "whelkListener"
    boolean enabled = true
    boolean isEnabled() {return enabled}
    void disable() {this.enabled = false}
    Date lastUpdate = null

    Class formatConverterClass
    Map converterParameters

    Listener(Whelk n, int nrOfHandlers, Class formatConverterClass, Map converterParameters) {
        this.otherwhelk = n
        this.numberOfHandlers = nrOfHandlers
        this.otherwhelk.addPluginIfNotExists(new Notifier(this))
        this.formatConverterClass = formatConverterClass
        this.converterParameters = converterParameters
        id = id + ", listening to $otherwhelk.prefix"
    }


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
        log.info("Starting $numberOfHandlers handlers.")
        for (int i = 0; i < this.numberOfHandlers; i++) {
            new Thread(new UpdateHandler(formatConverterClass, converterParameters)).start()
        }
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
                log.info("Found updates. Populating identifiers-list.")
                iter.each {
                    notify(it.identifier)
                }
            }
        }
    }

    void notify(URI identifier) {
        log.trace "Whelk $homewhelk.prefix notified of change in $identifier"
        if (!identifiers.contains(identifier)) {
            identifiers.push(identifier)
        }
    }

    void notify(Date timestamp) {
        log.trace "Whelk $homewhelk.prefix notified of change since $timestamp"
        boolean updatesReceived = false
        while (!updatesReceived) {
            def updates = otherwhelk.log(timestamp)
            def iter = updates.iterator()
            if (iter.hasNext()) {
                log.info("Found updates. Populating identifiers-list.")
                iter.each {
                    //log.debug("Adding identifier $it.identifier")
                    notify(it.identifier)
                }
                updatesReceived = true
            } else {
                log.debug("Got no updates, despite notification. Waiting a while ...")
                sleep(CHECK_AGAIN_DELAY)
            }
        }
    }

    @Synchronized
    URI nextIdentifier() {
        try {
            URI u = identifiers.pop() 
            log.debug("nextIdentifier returning $u. List contains " + identifiers.size() + " items.")
            return u
        } catch (NoSuchElementException nsee) {
            return null
        }
    }

    void enable() {
        this.enabled = true
        if (lastUpdate) {
            notify(lastUpdate)
        }
    }

    @Log
    class UpdateHandler implements Runnable {


        def converter

        UpdateHandler(Class convClass, Map params) {
            println "params: $params" 
            converter = convClass.getConstructor(Map.class).newInstance(params)
        }

        void run() {
            while (true) {
                def uri = nextIdentifier()
                if (uri) {
                    log.debug("Next is $uri")
                    convert(otherwhelk.get(uri))
                }
                sleep(CHECK_AGAIN_DELAY)
            }
            log.error("Thread is exiting ...")
        }

        void convert(Document doc) {
            log.debug("Converting document $doc.identifier ...")
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

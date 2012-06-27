package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.TeeOutputStream

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.persistance.*


@Log
class WhelkImpl extends BasicWhelk {

    WhelkImpl(pfx) {
        super(pfx)
    }

    boolean belongsHere(Document d) {
        return !d.identifier || d.identifier.toString().startsWith("/"+this.prefix+"/")
    }

    @Override
    URI store(Document d) {
        if (! belongsHere(d)) {
            throw new WhelkRuntimeException("Document does not belong here.")
        }
        try {
            return super.store(d)
        } catch (WhelkRuntimeException wre) {
            log.error("Failed to save document ${d.identifier}: " + wre.getMessage())
        }

        return null
    }

    @Override
    Iterable<LogEntry> log(Date since) {
        def query = new Query()
        //query.addFilter("timestamp", since)
        components.each {
            if (it instanceof Index) {
                it.performLogQuery(since)
            }
        }
        return null
    }

}

@Log
class ListeningWhelk extends WhelkImpl {
    def state

    ListeningWhelk(pfx) {
        super(pfx)
        state = new WhelkState(this)
        state.load()
    }


}

@Log
class WhelkState {

    def whelk
    final static String STORAGE_SUFFIX = "/.whelk/state"

    /** A list of whelks listening to notifications from this whelk. */
    def listeners = []
    /** A list of whelks that this whelk is listening to. */
    def notifiers = []

    WhelkState(Whelk w) {
        this.whelk = w
    }

    void addNotifier(whelk) {
        notifiers << whelk.toString()
    }
    void addListener(whelk) {
        listeners << whelk.toString()
    }

    void save() {
        log.debug("Saving whelkstate")
        def map = {}
        println "notifiers.size: " + notifiers.size()
        println "notifiers: " + notifiers
        map['listeners'] = listeners
        map['notifiers'] = notifiers
        println "Map is:\n" + map
        def builder = new groovy.json.JsonBuilder() 
        builder {
            "listeners"(listeners)
            "notifiers"(notifiers)
        }
        println "state is:\n" + builder.toPrettyString()
        def doc = this.whelk.createDocument().withData(builder.toString()).withIdentifier(new URI("/"+this.whelk.prefix+STORAGE_SUFFIX)).withContentType("application/json")
        this.whelk.store(doc)
    }

    void load() {
        def doc = this.whelk.get(new URI("/"+this.whelk.prefix+STORAGE_SUFFIX))
        if (doc) {
            log.debug("Loading whelkstate from storage")
            def slurper = new groovy.json.JsonSlurper()
            def result = slurper.parseText(doc.dataAsString)
            this.listeners = result.listeners
            this.notifiers = result.notifiers
        } else {
            log.debug("Didn't find any state.")
        }
    }

}

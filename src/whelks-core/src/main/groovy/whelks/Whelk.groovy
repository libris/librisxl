package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

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
            log.debug("[$prefix] Saving document with identifier $d.identifier")
            return super.store(d)
        } catch (WhelkRuntimeException wre) {
            log.error("Failed to save document ${d.identifier}: " + wre.getMessage())
        }

        return null
    }

    @Override
    public Iterable<LogEntry> log(Date since) {
        for (Component c : getComponents()) {
            if (c instanceof History) {
                return new LogIterable(((History)c).updates(since), c, since);
            }
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }
}

@Log
class LogIterable<LogEntry> implements Iterable {
    History history
    Collection<LogEntry> list
    boolean refilling = false
    boolean incomplete = false
    int offset = 0
    Object query

    LogIterable(Collection<LogEntry> i, History h, Object q) {
        this.list = i
        this.history = h
        this.query = q
        this.incomplete = (list.size == History.BATCH_SIZE)
    }

    Iterator<LogEntry> iterator() {
        return new LogIterator()
    }

    class LogIterator<LogEntry> implements Iterator {

        Iterator iter

        LogIterator() {
            iter = list.iterator()
        }

        boolean hasNext() {
            if (!iter.hasNext() && incomplete) {
                refill()
            }
            return iter.hasNext()
        }

        @Synchronized
        LogEntry next() {
            LogEntry n = iter.next();
            iter.remove();
            if (!iter.hasNext() && incomplete && !refilling) {
               refill()
            }
            return n
        }

        void remove() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Synchronized
        private void refill() {
            refilling = true
            offset = offset + History.BATCH_SIZE
            list = history.updates(query, offset)
            incomplete = (list.size() == History.BATCH_SIZE)
            iter = list.iterator()
            refilling = false
        }
    }
}

@Log
class ImportWhelk extends BasicWhelk {

    ImportWhelk(pfx) {
        super(pfx)
        log.info("Starting whelk '$pfx' in standalone import mode.")
    }

    static main(args) {
        if (args) {
            def prefix = args[0]
            def resource = (args.length > 1 ? args[1] : args[0])
            def whelk = new ImportWhelk(prefix)
            def date = (args.length > 2 ? new Date(new Long(args[2])) : null)
            whelk.addPlugin(new ElasticSearchClient(prefix))
            def importer = new se.kb.libris.whelks.imports.BatchImport(resource)
            importer.doImport(whelk, date)
        } else {
            println "Supply whelk-prefix and resource-name as arguments to commence import."
        }
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
        map['listeners'] = listeners
        map['notifiers'] = notifiers
        def builder = new groovy.json.JsonBuilder() 
        builder {
            "listeners"(listeners)
            "notifiers"(notifiers)
        }
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

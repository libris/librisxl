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

    URI store(Document d) {
        if (! belongsHere(d)) {
            throw new WhelkRuntimeException("Document does not belong here.")
        }
        try {
            log.info("[$prefix] Saving document with identifier $d.identifier")
            return super.store(d)
        } catch (WhelkRuntimeException wre) {
            log.error("Failed to save document ${d.identifier}: " + wre.getMessage())
        }

        return null
    }

    @Override
    void reindex() {
        log().each {
            def doc = get(it.identifier)
            for (def c : components) {
                if (c instanceof Index) {
                    c.index(doc)
                }
            }
        }
    }

    @Override
    public Iterable<LogEntry> log(Date since) {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                historyComponent = (History)c
            }
        }
        if (historyComponent) {
            return new LogIterable(historyComponent.updates(since), historyComponent, since);
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    @Override 
    public Iterable<LogEntry> log() {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                historyComponent = (History)c
            }
        }
        if (historyComponent) {
            return new LogIterable(historyComponent.updates(), historyComponent);
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
    def offset 
    def query

    LogIterable(History.HistoryUpdates u, History h, Object q=null) {
        this.list = u.updates
        this.offset = u.nextToken
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
            if (query) {
                offset = (offset ? offset : 0) + History.BATCH_SIZE
                list = history.updates(query, offset)
            } else {
                def hu = history.updates(offset)
                list = hu.updates
                offset = hu.nextToken
            }
            incomplete = (list.size() == History.BATCH_SIZE)
            iter = list.iterator()
            refilling = false
        }
    }
}

@Log
class ReindexingWhelk extends WhelkImpl {

    ReindexingWhelk(pfx) {
        super(pfx)
        log.info("Starting whelk '$pfx' in standalone reindexing mode.")
    }

    static main(args) {
        if (args) {
            def prefix = args[0]
            def resource = (args.length > 1 ? args[1] : args[0])
            def whelk = new ReindexingWhelk(prefix)
            def date = (args.length > 2 ? new Date(new Long(args[2])) : null)
            whelk.addPlugin(new ElasticSearchClientStorageIndexHistory(prefix))
            whelk.addPlugin(new MarcCrackerIndexFormatConverter())
            long startTime = System.currentTimeMillis()
            whelk.reindex()
            println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
        } else {
            println "Supply whelk-prefix and resource-name as arguments to commence reindexing."
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
            whelk.addPlugin(new ElasticSearchClientStorage(prefix))
            def importer = new se.kb.libris.whelks.imports.BatchImport(resource)
            long startTime = System.currentTimeMillis()
            def nrimports = importer.doImport(whelk, date)
            println "Imported $nrimports documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
        } else {
            println "Supply whelk-prefix and resource-name as arguments to commence import."
        }
    }
}

/* NOT USED ATM
@Log
class WhelkState {

    def whelk
    final static String STORAGE_SUFFIX = "/.whelk/state"

    def listeners = []
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
        def doc = this.whelk.createDocument()
            .withData(builder.toString())
            .withIdentifier(new URI("/"+this.whelk.prefix+STORAGE_SUFFIX))
            .withContentType("application/json")
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
*/

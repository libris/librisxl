package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import java.util.concurrent.*
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.TeeOutputStream

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.persistance.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.plugin.external.*

import org.codehaus.jackson.map.*

@Log
class WhelkImpl extends BasicWhelk {

    WhelkImpl(String pfx) {
        super(pfx)
    }

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.identifier = mintIdentifier(doc)
        }
        if (!d.identifier.toString().startsWith("/"+this.prefix+"/")) {
            throw new WhelkRuntimeException("Document with id ${d.identifier} does not belong in whelk with prefix ${this.prefix}")
        }
        return d
    }

    @Override
    URI store(Document doc) {
        doc = sanityCheck(doc)

        for (storage in storages) {
            storage.store(doc, this.prefix)
        }

        addToIndex(doc)
        addToQuadStore(doc)

        return doc.identifier
    }

    void addToIndex(doc) {
        log.debug("Adding to indexes")
        def docs = []
        for (ifc in getIndexFormatConverters()) {
            log.trace("Calling indexformatconverter $ifc")
            docs.addAll(ifc.convert(doc))
        }
        if (!docs) {
            docs.add(doc)
        }
        for (d in docs) {
            for (idx in indexes) {
                idx.index(d, this.prefix)
            }
        }
    }

    void addToQuadStore(doc) {}

    @Deprecated
    Document createDocument() {
        return new BasicDocument()
    }

    Document performStorageFormatConversion(Document doc) {
        for (fc in formatConverters) {
            log.debug("Running formatconverter $fc")
            doc = fc.convert(doc)
        }
        return doc
    }

    @Override
    Document createDocument(data, metadata) {
        log.debug("Creating document")
        def doc = new BasicDocument().withData(data)
        metadata.each { param, value ->
            log.debug("Adding $param = $value")
            doc = doc."with${param.capitalize()}"(value)
        }
        performStorageFormatConversion(doc)
        for (lf in linkFinders) {
            log.debug("Running linkfinder $lf")
            for (link in lf.findLinks(doc)) {
                doc.withLink(link)
            }
        }
        return doc
    }

    @Override
    void reindex(fromStorage=null) {
        int counter = 0
        Storage scomp
        if (fromStorage) {
            scomp = components.find { it instanceof Storage && it.id == fromStorage }
        } else {
            scomp = components.find { it instanceof Storage }
        }
        Index icomp = components.find { it instanceof Index }
        def ifcs = plugins.findAll { it instanceof IndexFormatConverter }

        log.info("Using $scomp as storage source for reindex.")

        long startTime = System.currentTimeMillis()
        List<Document> docs = new ArrayList<Document>()
        def executor = newScalingThreadPoolExecutor(1,50,60)
        try {
            for (Document doc : scomp.getAll(this.prefix)) {
                counter++
                docs.add(doc)
                if (counter % History.BATCH_SIZE == 0) {
                    long ts = System.currentTimeMillis()
                    log.info "(" + ((ts - startTime)/History.BATCH_SIZE) + ") New batch, indexing document with id: ${doc.identifier}. Velocity: " + (counter/((ts - startTime)/History.BATCH_SIZE)) + " documents per second. $counter total sofar."
                    def idocs = new ArrayList<Document>(docs)
                    executor.execute(new Runnable() {
                        public void run() {
                            for (ifc in ifcs) {
                                idocs = ifc.convertBulk(idocs)
                            }
                            log.debug("Current pool size: " + executor.getPoolSize() + " current active count " + executor.getActiveCount())
                            log.info("Indexing "+idocs.size()+" documents ... "+"Whelk prefix: "+this.getPrefix())
                            icomp.index(idocs, this.getPrefix())
                        }
                    })
                    docs.clear()
                }
            }
            if (docs.size() > 0) {
                log.info "Indexing remaining " + docs.size() + " documents."
                for (ifc in ifcs) {
                    log.trace("Running converter $ifc")
                    docs = ifc.convertBulk(docs)
                }
                if (icomp == null) {
                    log.warn("No index components configured for ${this.prefix} whelk.")
                    counter = 0
                } else {
                    log.info "Indexing remaining " + docs.size() + " documents."
                    icomp?.index(docs, this.prefix)
                }
            }
        } finally {
            executor.shutdown()
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        }
        log.debug "Reindexed $counter documents"
    }

    @Override
    public Iterable<Document> log(Date since) {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                return ((History)c).updates(since)
            }
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    public ExecutorService newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        ScalingQueue queue = new ScalingQueue()
        ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }

}

@Log
class CombinedWhelk extends WhelkImpl {

    String pfxs
    def rules = [:]

    CombinedWhelk(String pfx) {
        super(pfx)
    }

    /**
     * Set by json configuration via WhelkInitializer.
     */
    void setPrefixes(List pfxs) {
        log.trace("Setting indexes: $pfxs")
        this.pfxs = pfxs.join(",")
    }

    void setStorageRules(Map r) {
        log.trace("Setting storagerules: $rules")
        this.rules = r
    }

    @Override
    Document get(URI uri) {
        if (!rules.preferredRetrieveFrom) {
            log.trace("No preferred storage. Using superclass get().")
            return super.get(uri)
        } else {
            log.trace("Attempting retrieve from ${rules.preferredRetrieveFrom}")
            return storages.find { it.id == rules.preferredRetrieveFrom }?.get(uri, this.prefix)
        }
    }

    @Override
    URI store(Document doc) {
        if (!rules.storeByFormat) {
            log.trace("No direction rules for storing. Using superclass store().")
            return super.store(doc)
        } else {
            doc = sanityCheck(doc)

            def usedStorages = []

            for (storage in storages) {
                if (rules.storeByFormat[doc.format] == storage.id) {
                    log.debug("Storing document with format ${doc.format} in storage with id ${storage.id}")
                    storage.store(doc, this.pfxs)
                    usedStorages << storage
                }
            }

            def remainingStorages = new ArrayList(storages)
            for (us in usedStorages) {
                remainingStorages.remove(us)
            }
            log.trace("Remaining storages : $remainingStorages")

            doc = performStorageFormatConversion(doc)

            for (storage in remainingStorages) {
                if (!rules.storeByFormat[doc.format] || rules.storeByFormat[doc.format] == storage.id) {
                    log.debug("Storing document with format ${doc.format} in storage with id ${storage.id}")
                    storage.store(doc, this.pfxs)
                }
            }

            addToIndex(doc)
            addToQuadStore(doc)

            return doc.identifier
        }
    }

    Document createDocument(byte[] data, Map<String, Object> metadata) {
        return createDocument(new String(data), metadata)
    }

    @Override
    Document createDocument(data, metadata) {
        log.debug("Creating document")
        def doc = new BasicDocument().withData(data)
        metadata.each { param, value ->
            log.trace("Adding $param = $value")
            doc = doc."with${param.capitalize()}"(value)
        }
        return doc
    }

    @Override
    Iterable<URI> bulkStore(Iterable<Document> docs) {
        def list = []
        for (doc in docs) {
            list << store(doc)
        }
        return list
    }

    @Override
    protected void initializePlugins() {
        for (plugin in plugins) {
            if (rules.dontInitialize?.contains(plugin.id)) {
                log.trace("Combined whelk does not initialize plugin ${plugin.id}. The rules says so.")
            } else {
                log.trace("[${this.prefix}] Initializing ${plugin.id}")
                plugin.init(this.prefix)
            }

        }
    }

    @Override
    SearchResult query(Query q, String indexType) {
        log.trace("query intercepted: $q, $indexType")
        return super.query(q, this.pfxs, indexType)
    }
}

/**
 * Used by the local "mock" whelk setup.
 */
@Log
class ReindexOnStartupWhelk extends WhelkImpl {

    ReindexOnStartupWhelk(String pfx) {
        super(pfx)
    }

    @Override
    public void init() {
        log.info("Indexing whelk ${this.prefix}.")
        reindex()
        log.info("Whelk reindexed.")
    }
}

class Tool {
    static Date parseDate(repr) {
        if (!repr.number) {
            return Date.parse("yyyy-MM-dd'T'hh:mm:ss", repr)
        } else {
            def tstamp = new Long(repr)
            if (tstamp < 0) // minus in days
                return new Date() + (tstamp as int)
            else // time in millisecs
                return new Date(tstamp)
        }
    }
}

@Log
class WhelkOperator {

    static main(args) {
        def operation = (args.length > 0 ? args[0] : null)
        def whelk = (args.length > 2 ? (new WhelkInitializer(new URI(args[2]).toURL().newInputStream()).getWhelks().find { it.prefix == args[1] }) : null)
        def resource = (args.length > 3 ? args[3] : whelk?.prefix)
        def since = (args.length > 4 ? Tool.parseDate(args[4]) : null)
        def numberOfDocs = (args.length > 5 ? args[5].toInteger() : null)
        long startTime = System.currentTimeMillis()
        long time = 0
        if (operation == "import") {
            def importer = new BatchImport(resource)
            def nrimports = importer.doImport(whelk, since, numberOfDocs)
            float elapsed = ((System.currentTimeMillis() - startTime) / 1000)
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."

        } else if (operation == "reindex") {
            if (args.length > 3) { // Reindex a single document
                println "Reindexing document with identifier $resource"
                def document = whelk.get(new URI(resource))
                whelk.store(document)
            } else {
                whelk.reindex()
                println "Reindexed documents in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds."
            }
        } else if (operation == "populate" || operation == "rebalance") {
            def target = (args.length > 2 ? (new WhelkInitializer(new URI(args[2]).toURL().newInputStream()).getWhelks().find { it.prefix == resource }) : null)
            int count = 0
            def docs = []
            for (doc in whelk.log()) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    log.info("Storing "+ docs.size()+ " documents in " + (target ? target.prefix : "all components") + " ... ($count total)")
                    if (target) {
                        target.store(docs)
                    } else {
                        whelk.store(docs)
                    }
                    docs = []
                }
            }
            if (docs.size() > 0) {
                count += docs.size()
                if (target) {
                    target.store(docs)
                } else {
                    whelk.store(docs)
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            println "Whelk ${whelk.prefix} is ${operation}d. $count documents in $time seconds."
        } else if (operation == "benchmark") {
            int count = 0
            def docs = []
            for (doc in whelk.log()) {
                docs << doc
                count++
                if (count % 1000 == 0) {
                    time = (System.currentTimeMillis() - startTime)/1000
                    log.info("Retrieved "+ docs.size()+ " documents from $whelk ... ($count total). Time elapsed: ${time}. Current velocity: "+ (count/time) + " documents / second.")
                    docs = []
                }
            }
            time = (System.currentTimeMillis() - startTime)/1000
            log.info("$count documents read. Total time elapsed: ${time} seconds. That's " + (count/time) + " documents / second.")
        } else {
            println "Usage: <import|reindex|rebalance|populate> <whelkname> <config-url> [resource (for import)|target (for populate)] [since (for import)]"
        }
    }
}

@Log
class ResourceWhelk extends WhelkImpl {

    ResourceWhelk(String prefix) {
        super(prefix)
    }

}

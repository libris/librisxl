package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

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
class StandardWhelk implements Whelk {

    String prefix
    List<Plugin> plugins = new ArrayList<Plugin>()


    StandardWhelk(String pfx) {
        this.prefix = pfx
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

    @Override
    Iterable<URI> bulkStore(Iterable<Document> docs) {
        uris =[]
        for (doc in docs) {
            uris << store(doc)
        }
        return uris
    }

    @Override
    Document get(URI uri) {
        return plugins.find { it instanceof Storage }?.get(uri, this.prefix)
    }

    @Override
    void delete(URI uri) {
        components.each {
            try {
                it.delete(uri, this.prefix)
            } catch (RuntimeException rte) {
                log.warn("Component ${it.id} failed delete: ${rte.message}")
            }
        }
    }

    @Override
    SearchResult<? extends Document> query(Query query) {
        return plugins.find { it instanceof Index }?.query(query, this.prefix)
    }

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.identifier = mintIdentifier(d)
        }
        if (!d.identifier.toString().startsWith("/"+this.prefix+"/")) {
            throw new WhelkRuntimeException("Document with id ${d.identifier} does not belong in whelk with prefix ${this.prefix}")
        }
        return d
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
        for (idx in indexes) {
            for (d in docs) {
                idx.index(d, this.prefix)
            }
        }
    }

    void addToQuadStore(doc) {}

    @Override
    Iterable<Document> loadAll(Date since = null) {
    }

    @Override
    Document createDocument(byte[] data, Map metadata) { return createDocument(new String(data), metadata) }
    Document createDocument(data, metadata) {
        log.debug("Creating document")
        def doc = new BasicDocument().withData(data)
        metadata.each { param, value ->
            log.debug("Adding $param = $value")
            doc = doc."with${param.capitalize()}"(value)
        }
        doc = performStorageFormatConversion(doc)
        for (lf in linkFinders) {
            log.debug("Running linkfinder $lf")
            for (link in lf.findLinks(doc)) {
                doc.withLink(link)
            }
        }
        return doc
    }

    Document performStorageFormatConversion(Document doc) {
        for (fc in formatConverters) {
            log.debug("Running formatconverter $fc")
            doc = fc.convert(doc)
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
    void addPlugin(Plugin plugin) {
        log.trace("[${this.prefix}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.prefix)
        this.plugins.add(plugin)
    }

    // Sugar methods
    def getStorages() { return plugins.findAll { it instanceof Storage } }
    def getIndexes() { return plugins.findAll { it instanceof Index } }
    def getAPIs() { return plugins.findAll { it instanceof API } }
    def getFormatConverters() { return plugins.findAll { it instanceof FormatConverter } }
    def getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter } }
    def getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}

    @Deprecated
    public Iterable<Document> log(Date since) {
        History historyComponent = null
        for (Component c : getComponents()) {
            if (c instanceof History) {
                return ((History)c).updates(since)
            }
        }
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    @Deprecated
    public ExecutorService newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        ScalingQueue queue = new ScalingQueue()
        ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }



}

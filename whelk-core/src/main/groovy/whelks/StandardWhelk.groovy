package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.net.URI
import java.net.URISyntaxException

import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*

import se.kb.libris.conch.Tools

import org.codehaus.jackson.map.*

@Log
class StandardWhelk implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()
    List<Storage> storages = new ArrayList<Storage>()
    Index index
    Map<String,FormatConverter> formatConverters = new HashMap<String,FormatConverter>()
    List<IndexFormatConverter> indexFormatConverters = new ArrayList<IndexFormatConverter>()

    private Map<String, List<BlockingQueue>> queues
    private List<Thread> prawnThreads
    boolean prawnsActive = false

    // Set by configuration
    URI docBaseUri

    StandardWhelk(String id) {
        this.id = id
        queues = [:].withDefault { new ArrayList<BlockingQueue>() }
        prawnThreads = []
    }

    void setDocBaseUri(String uri) {
        this.docBaseUri = new URI(uri)
    }

    @Override
    URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata) {
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
        log.debug("Created new document with timestamp ${new Date(doc.timestamp)}")
        return add(doc)
    }

    @Override
    @groovy.transform.CompileStatic
    URI add(Document doc) {
        log.trace("Add single document ${doc.identifier}")
        doc = addToStorage(doc)
        addToGraphStore([doc])
        addToIndex([doc])

        return new URI(doc.identifier)
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs) {
        log.debug("Bulk add ${docs.size()} document")
        List<Document> convertedDocs = []
        for (doc in docs) {
            try {
                convertedDocs.add(addToStorage(doc))
            } catch (WhelkAddException wae) {
                log.warn(wae.message)
            }
        }
        try {
            log.trace("${convertedDocs.size()} docs left to triplify ...")
            addToGraphStore(convertedDocs)
        } catch (Exception e) {
            log.error("Failed adding documents to graphstore: ${e.message}", e)
        }
        try {
            log.trace("${convertedDocs.size()} docs left to index ...")
            addToIndex(convertedDocs)
        } catch (Exception e) {
            log.error("Failed indexing documents: ${e.message}", e)
        }
    }

    @Override
    Document get(URI uri, version=null, List contentTypes=[], boolean dontAlertThePrawns = false) {
        Document doc
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage ${s.id}.")
                doc = s.get(uri, version)
            }
        }
        if (!doc) {
            doc = storage.get(uri, version)
        }

        if (prawnsActive && !dontAlertThePrawns && doc?.identifier && queues) {
            log.debug("Adding ${doc.identifier} to prawn queue")
            for (queue in queues.get(GetPrawn.TRIGGER)) {
                queue.put(doc)
            }
        }
        return doc
    }

    @Override
    void remove(URI uri) {
        components.each {
            try {
                ((Component)it).delete(uri)
            } catch (RuntimeException rte) {
                log.warn("Component ${((Component)it).id} failed delete: ${rte.message}")
            }
        }
    }

    @Override
    SearchResult search(Query query) {
        return index?.query(query)
    }

    @Override
    InputStream sparql(String query) {
        return sparqlEndpoint?.sparql(query)
    }

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.withIdentifier(mintIdentifier(d))
            log.debug("Document was missing identifier. Setting identifier ${d.identifier}")
        }
        d.timestamp = new Date().getTime()
        return d
    }

    /**
     * Handles conversion for a document and stores each converted version into suitable storage.
     * @return The final resulting document, after all format conversions
     */
    @groovy.transform.CompileStatic
    Document addToStorage(Document doc, boolean withConversion = true) {
        boolean stored = false
        if (withConversion) {
            FormatConverter formatconverter = (FormatConverter)formatConverters.get(doc.contentType, null)
            if (formatconverter) {
                try {
                    doc = formatconverter.convert(doc)
                } catch (Exception e) {
                    log.error("Conversion failed for ${doc.identifier}.", e)
                }
            }
        }
        for (storage in getStorages(doc.contentType)) {
            try {
                stored = (storage.store(doc) || stored)
            } catch (DocumentException de) {
                if (de.exceptionType == DocumentException.IDENTICAL_DOCUMENT) {
                    log.debug("Identical document already in storage.")
                    stored = true
                } else {
                    throw de
                }
            }
        }
        if (!stored && storages.size() > 0) {
            throw new PluginConfigurationException("No storages available for documents with contentType ${doc.contentType}.")
        }
        return doc
    }

    @groovy.transform.CompileStatic
    void addToIndex(final List<Document> docs) {
        List<IndexDocument> idxDocs = []
        if (index) {
            log.debug("Number of documents to index: ${docs.size()}")
            if (indexFormatConverters.size()) {
                for (doc in docs) {
                    for (ifc in getIndexFormatConverters()) {
                        log.trace("Running indexformatconverter ${ifc.id} on document with ctype ${doc.contentType}")
                        idxDocs.addAll(ifc.convert(doc))
                    }
                }
            }
            if (idxDocs) {
                try {
                    index.bulkIndex(idxDocs)
                } catch (Exception e) {
                    throw new WhelkAddException("Failed to add documents to index ${index.id}.".toString(), e, idxDocs.collect { ((IndexDocument)it).identifier })
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No documents to index.")
            }
        } else if (log.isDebugEnabled()) {
            log.warn("No index configured for this whelk.")
        }
    }

    void addToGraphStore(final List<Document> docs, List<String> gStores = null) {
        def activeGraphStores = (gStores ? graphStores.findAll { it.id in gStores } : graphStores)
        if (activeGraphStores.size() > 0) {
            log.debug("addToGraphStore ${docs.size()}")
            log.debug("Adding to graph stores")
            Map<String, RDFDescription> dataDocs = new HashMap<String, RDFDescription>()
            for (doc in docs) {
                for (rc in getRDFFormatConverters()) {
                    log.trace("Running indexformatconverter $rc")
                    dataDocs.putAll(rc.convert(doc))
                }
            }
            if (dataDocs) {
                try {
                    for (store in activeGraphStores) {
                        if (store instanceof BatchGraphStore) {
                            log.debug("Batch updating graphstore. $dataDocs")
                            store.batchUpdate(dataDocs)
                        } else {
                            dataDocs.each {
                                store.update(docBaseUri.resolve(it.key), it.value)
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new WhelkAddException("Failed to add documents to graph stores ${activeGraphStores}.", e, dataDocs.collect { it.key } )
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No graphs to update.")
            }
        } else if (graphStores.size() > 0) {
            throw new PluginConfigurationException("You have graphstores configured, but none available for these documents.")
        }
    }

    @Override
    Iterable<Document> loadAll(Date since) { return loadAll(null, since, null)}

    @Override
    Iterable<Document> loadAll(String dataset = null, Date since = null, String storageId = null) {
        def st
        if (storageId) {
            st = getStorages().find { it.id == storageId }
        } else {
            st = getStorage()
        }
        if (st) {
            log.debug("Loading "+(dataset ? dataset : "all")+" "+(since ?: "")+" from storage ${st.id}")
            return st.getAll(dataset, since)
        } else {
            throw new WhelkRuntimeException("Couldn't find storage. (storageId = $storageId)")
        }
    }

    /*
    void findLinks(String dataset) {
        log.info("Trying to findLinks for ${dataset}... ")
        for (doc in loadAll(dataset)) {
            log.debug("Finding links for ${doc.identifier} ...")
            for (linkFinder in getLinkFinders()) {
                log.debug("LinkFinder ${linkFinder}")
                for (link in linkFinder.findLinks(doc)) {
                    doc.withLink(link.identifier.toString(), link.type)
                }
            }
            add(doc)
       }
    }

    void runFilters(String dataset) {
        log.info("Running filters for ${dataset} ...")
        long startTime = System.currentTimeMillis()
        int counter = 0
        def docs = []
        for (doc in loadAll(dataset)) {
            for (filter in getFilters()) {
                log.debug("Running filter ${filter.id}")
                docs << addToStorage(filter.filter(doc))
                if (++counter % 1000 == 0) {
                    addToGraphStore(docs)
                    try {
                        addToIndex(docs)
                    } catch (WhelkAddException wae) {
                        log.info("Failed indexing identifiers: ${wae.failedIdentifiers}")
                    }
                    docs = []
                }
                if (log.isInfoEnabled()) {
                    Tools.printSpinner("Filtering ${this.id}. ${counter} documents sofar.", counter)
                }
            }
        }
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            addToGraphStore(docs)
            addToIndex(docs)
        }
        log.info("Filtered $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
    }
    */

    @Override
    void flush() {
        log.info("Flushing data.")
        // TODO: Implement storage and graphstore flush if necessary
        index?.flush()
    }

    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.id}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.id)
        if (plugin instanceof Prawn) {
            prawnsActive = true
            log.debug("[${this.id}] Starting Prawn: ${plugin.id}")
            log.debug("Adding to queue ${plugin.trigger}")
            queues.get(plugin.trigger).add(plugin.getQueue())
            def t = new Thread(plugin)
            t.start()
            prawnThreads << t
        }
        if (plugin instanceof Storage) {
            if (((Storage)plugin).isHybrid() && index) {
                log.debug("Added index to storage ${plugin.id}")
                plugin.index = index
            }
            this.storages.add(plugin)
        } else if (plugin instanceof Index) {
            if (index) {
                throw new PluginConfigurationException("Index ${index.id} already configured for whelk ${this.id}.")
            }
            for (st in storages) {
                if (st.isHybrid()) {
                    log.debug("Added index to storage ${st.id}")
                    st.index = index
                }
            }
            this.index = plugin
        } else if (plugin instanceof FormatConverter) {
            this.formatConverters.put(plugin.requiredContentType, plugin)
        } else if (plugin instanceof IndexFormatConverter) {
            this.indexFormatConverters.add(plugin)
        }
        // And always add to plugins
        this.plugins.add(plugin)
    }


    void stopPrawns() {
        prawnsActive = false
        for (t in prawnThreads) {
            t.interrupt()
        }
    }

    void startPrawns() {
        prawnThreads = []
        for (plugin in plugins) {
            if (plugin instanceof Prawn) {
                prawnsActive = true
                log.debug("[${this.id}] Starting Prawn: ${plugin.id}")
                queues.get(plugin.trigger).add(plugin.getQueue())
                def t = new Thread(plugin)
                t.start()
                prawnThreads << t
            }
        }
    }

    @Override
    URI mintIdentifier(Document d) {
        URI identifier
        for (minter in uriMinters) {
            identifier = minter.mint(d)
        }
        if (!identifier) {
            try {
                //identifier = new URI("/"+id.toString() +"/"+ UUID.randomUUID());
                // Temporary to enable kitin progress
                identifier = new URI("/bib/"+ UUID.randomUUID());
            } catch (URISyntaxException ex) {
                throw new WhelkRuntimeException("Could not mint URI", ex);
            }
        }
        return identifier
    }

    // Sugar methods
    List<Component> getComponents() { return plugins.findAll { it instanceof Component } }

    Storage getStorage() { return storages.get(0) }
    List<Storage> getStorages(String rct) { return storages.findAll { it.handlesContent(rct) } }
    Storage getStorage(String rct) { return storages.find { it.handlesContent(rct) } }

    List<GraphStore> getGraphStores() { return plugins.findAll { it instanceof GraphStore } }
    GraphStore getGraphStore() { return plugins.find { it instanceof GraphStore } }
    List<SparqlEndpoint> getSparqlEndpoints() { return plugins.findAll { it instanceof SparqlEndpoint } }
    SparqlEndpoint getSparqlEndpoint() { return plugins.find { it instanceof SparqlEndpoint } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }

    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }

}

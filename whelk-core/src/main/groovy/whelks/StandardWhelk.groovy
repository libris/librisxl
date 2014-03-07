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

    private List<BlockingQueue> queues

    // Set by configuration
    URI docBaseUri

    StandardWhelk(String id) {
        this.id = id
        queues = new ArrayList<BlockingQueue>()
    }

    void setDocBaseUri(String uri) {
        this.docBaseUri = new URI(uri)
    }

    @Override
    URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata) {
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
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
    Document get(URI uri, version=null, List contentTypes=[]) {
        Document doc
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage.")
                doc = s.get(uri, version)
            }
        }
        if (!doc) {
            doc = storage.get(uri, version)
        }

        if (doc?.identifier && queues) {
            log.debug("Adding ${doc.identifier} to prawn queue")
            for (queue in queues) {
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
        return indexes.get(0)?.query(query)
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
    Document addToStorage(Document doc, String excemptStorage = null) {
        boolean stored = false
        Map<String,Document> docs = [(doc.contentType): doc]
        log.trace("Available formatconverters: " + formatConverters.collect { ((Plugin)it).id })
        for (fc in formatConverters) {
            log.trace("Running formatconverter $fc for ${doc.contentType}")
            doc = fc.convert(doc)
            log.trace("Document has ctype ${doc.contentType} after conversion.")
            docs.put(doc.contentType, doc)
        }
        for (d in docs.values()) {
            for (st in getStorages(d.contentType)) {
                if (st.id != excemptStorage) {
                    log.trace("[${this.id}] Sending doc ${d.identifier} with ct ${d.contentType} to ${st.id}")
                    try {
                        stored = (st.store(d) || stored)
                    } catch (DocumentException de) {
                        if (de.exceptionType == DocumentException.IDENTICAL_DOCUMENT) {
                            log.debug("Identical document already in storage.")
                            stored = true
                        } else {
                            throw de
                        }
                    }
                }
            }
        }
        if (!stored) {
            throw new PluginConfigurationException("You have storages configured, but none available for these documents.")
        }
        log.trace("Final conversion left document in ctype ${doc.contentType}")
        return doc
    }

    @groovy.transform.CompileStatic
    void addToIndex(List<Document> docs, List<String> sIndeces = null) {
        List<IndexDocument> idxDocs = []
        def activeIndexes = (sIndeces ? indexes.findAll { ((Index)it).id in sIndeces } : indexes)
        if (activeIndexes.size() > 0) {
            log.debug("Number of documents to index: ${docs.size()}")
            for (doc in docs) {
                for (ifc in getIndexFormatConverters()) {
                    log.trace("Running indexformatconverter ${ifc.id} on document with ctype ${doc.contentType}")
                    idxDocs.addAll(ifc.convert(doc))
                }
            }
            if (idxDocs) {
                try {
                    for (idx in indexes) {
                        log.trace("[${this.id}] ${idx.id} qualifies for indexing")
                        idx.bulkIndex(idxDocs)
                    }
                } catch (Exception e) {
                    throw new WhelkAddException("Failed to add documents to index ${indexes}.".toString(), e, idxDocs.collect { ((IndexDocument)it).identifier })
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No documents to index.")
            }
        } else if (indexes.size() > 0) {
            throw new PluginConfigurationException("You have indices configured, but none available for these documents.")
        }
    }

    void addToGraphStore(List<Document> docs, List<String> gStores = null) {
        def activeGraphStores = (gStores ? graphStores.findAll { it.id in gStores } : graphStores)
        if (activeGraphStores.size() > 0) {
            log.debug("addToGraphStore ${docs.size()}")
            log.debug("Adding to graph stores")
            List<Document> dataDocs = []
            for (doc in docs) {
                for (rc in getRDFFormatConverters()) {
                    log.trace("Running indexformatconverter $rc")
                    dataDocs.addAll(rc.convert(doc))
                }
            }
            if (dataDocs) {
                try {
                    for (store in activeGraphStores) {
                        dataDocs.each {
                            store.update(docBaseUri.resolve(it.identifier), it)
                        }
                    }
                } catch (Exception e) {
                    throw new WhelkAddException("Failed to add documents to graph stores ${activeGraphStores}.", e, dataDocs.collect { it.identifier })
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No graphs to update.")
            }
        } else if (graphStores.size() > 0) {
            throw new PluginConfigurationException("You have graphstores configured, but none available for these documents.")
        }
    }

    @Override
    Iterable<Document> loadAll(Date since) { return loadAll(null, null, since)}

    @Override
    Iterable<Document> loadAll(String dataset = null, String storageId = null, Date since = null) {
        def st
        if (storageId) {
            st = getStorages().find { it.id == storageId }
        } else {
            st = getStorage()
        }
        if (st) {
            log.debug("Loading "+(dataset ? dataset : "all")+" from storage ${st.id}")
            return st.getAll(dataset)
        } else {
            throw new WhelkRuntimeException("Couldn't find storage. (storageId = $storageId)")
        }
    }

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
                docs << addToStorage(filter.doFilter(doc))
                //doc = filter.doFilter(doc)
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

    @Override
    void flush() {
        log.info("Flushing data.")
        // TODO: Implement storage and graphstore flush if necessary
        for (i in indexes) {
            i.flush()
        }
    }

    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.id}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.id)
        if (plugin instanceof Prawn) {
            log.debug("[${this.id}] Starting Prawn: ${plugin.id}")
            queues.add(plugin.getQueue())
            (new Thread(plugin)).start()
        }
        this.plugins.add(plugin)
    }

    @Override
    URI mintIdentifier(Document d) {
        URI identifier
        for (minter in uriMinters) {
            identifier = minter.mint(d)
        }
        if (!identifier) {
            try {
                identifier = new URI("/"+id.toString() +"/"+ UUID.randomUUID());
            } catch (URISyntaxException ex) {
                throw new WhelkRuntimeException("Could not mint URI", ex);
            }
        }
        return identifier
    }

    // Sugar methods
    List<Component> getComponents() { return plugins.findAll { it instanceof Component } }
    List<Storage> getStorages() { return plugins.findAll { it instanceof Storage } }
    Storage getStorage() { return plugins.find { it instanceof Storage } }
    List<Storage> getStorages(String rct) { return plugins.findAll { it instanceof Storage && it.handlesContent(rct) } }
    Storage getStorage(String rct) { return plugins.find { it instanceof Storage && it.handlesContent(rct) } }
    List<Index> getIndexes() { return plugins.findAll { it instanceof Index } }
    List<GraphStore> getGraphStores() { return plugins.findAll { it instanceof GraphStore } }
    GraphStore getGraphStore() { return plugins.find { it instanceof GraphStore } }
    List<SparqlEndpoint> getSparqlEndpoint() { return plugins.findAll { it instanceof SparqlEndpoint } }
    SparqlEndpoint getSparqlEndpoint() { return plugins.find { it instanceof SparqlEndpoint } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }
    List<FormatConverter> getFormatConverters() { return plugins.findAll { it instanceof FormatConverter }}
    List<IndexFormatConverter> getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter }}
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }

}

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
class StandardWhelk extends AbstractWhelkServlet implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()
    List<Storage> storages = new ArrayList<Storage>()
    Index index
    GraphStore graphStore

    List<LinkExpander> linkExpanders = new ArrayList<LinkExpander>()

    private Map<String, List<BlockingQueue>> queues
    private List<Thread> prawnThreads
    boolean prawnsActive = false

    // Set by configuration
    URI docBaseUri

    /*
    StandardWhelk() {
        this.id = "libris"
    }

    StandardWhelk(String id) {
        this.id = id
        queues = [:].withDefault { new ArrayList<BlockingQueue>() }
        prawnThreads = []
    }*/

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
        log.debug("Add single document ${doc.identifier}")
            try {
                if (!doc.data || doc.data.length < 1) {
                    throw new DocumentException(DocumentException.EMPTY_DOCUMENT, "Tried to store empty document.")
                }
                for (storage in getStorages(doc.contentType)) {
                    storage.add(doc)
                }
                // Now handled by listeners
                /*
                if (index) {
                index.add(doc)
                }
                if (graphStore) {
                graphStore.add(doc)
                }
                */
            } catch (Exception e) {
            log.error("Failed to add document ${doc?.identifier}", e)
                throw e
        }
        return new URI(doc.identifier)
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs, String contentType) {
        log.debug("Bulk add ${docs.size()} document")
        for (storage in storages) {
            storage.bulkAdd(docs, contentType)
        }
        /*
        if (index) {
            index.bulkAdd(docs, contentType)
        }
        if (graphStore) {
            graphStore.bulkAdd(docs, contentType)
        }
        */
    }

    @Override
    Document get(URI uri, version=null, List contentTypes=[], boolean expandLinks = true) {
        Document doc = null
        for (contentType in contentTypes) {
            log.trace("Looking for $contentType storage.")
            def s = getStorage(contentType)
            if (s) {
                log.debug("Found $contentType storage ${s.id}.")
                doc = s.get(uri, version)
                break
            }
        }
        // TODO: Check this
        if (!doc) {
            doc = storage.get(uri, version)
        }


        if (expandLinks) {
            LinkExpander le = getLinkExpanderFor(doc)
            if (le) {
                String origchecksum = doc.checksum
                doc = le.expand(doc)
                /*
                if (doc.checksum != origchecksum) {
                    log.debug("Indexing expanded doc.")
                    index.add(doc)
                }
                */
            }
        }

        return doc
    }

    @Override
    void remove(URI uri) {
        components.each {
            try {
                ((Component)it).remove(uri)
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
        // TODO: Self describing
        if (d.contentType == "application/ld+json") {
            Map dataMap = d.dataAsMap
            if (dataMap.get("@id") != d.identifier) {
                dataMap.put("@id", d.identifier)
                d.withData(dataMap)
            }
        }
        d.timestamp = new Date().getTime()
        return d
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
            this.storages.add(plugin)
        } else if (plugin instanceof Index) {
            if (index) {
                throw new PluginConfigurationException("Index ${index.id} already configured for whelk ${this.id}.")
            }
            this.index = plugin
        } else if (plugin instanceof GraphStore) {
            if (graphStore) {
                throw new PluginConfigurationException("GraphStore ${index.id} already configured for whelk ${this.id}.")
            }
            this.graphStore = plugin
        } else if (plugin instanceof LinkExpander) {
            this.linkExpanders.add(plugin)
        }
        // And always add to plugins
        this.plugins.add(plugin)
        plugin.init(this.id)
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

    List<SparqlEndpoint> getSparqlEndpoints() { return plugins.findAll { it instanceof SparqlEndpoint } }
    SparqlEndpoint getSparqlEndpoint() { return plugins.find { it instanceof SparqlEndpoint } }
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
    List<Filter> getFilters() { return plugins.findAll { it instanceof Filter }}
    Importer getImporter(String id) { return plugins.find { it instanceof Importer && it.id == id } }
    List<Importer> getImporters() { return plugins.findAll { it instanceof Importer } }
    LinkExpander getLinkExpanderFor(Document doc) { return linkExpanders.find { it.valid(doc) } }


    // Maintenance whelk methods
    protected void setId(String id) {
        this.id = id
    }
}

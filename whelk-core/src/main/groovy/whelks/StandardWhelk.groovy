package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.util.UUID
import java.net.URI
import java.net.URISyntaxException

import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.conch.Tools

import org.codehaus.jackson.map.*

@Log
class StandardWhelk implements Whelk {

    String id
    List<Plugin> plugins = new ArrayList<Plugin>()

    // Set by configuration
    URI docBaseUri


    StandardWhelk(String id) {
        this.id = id
        // Start executorservice
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
    URI add(Document doc, boolean justStore = false) {
        boolean stored = false
        Map<String,Document> docs = [(doc.contentType): doc]
        for (fc in formatConverters) {
            log.trace("Running formatconverter $fc for ${doc.contentType}")
            doc = fc.convert(doc)
            docs.put(doc.contentType, doc)
        }
        for (d in docs.values()) {
            for (storage in getStorages(d.contentType)) {
                log.trace("Sending doc ${d.identifier} with ct ${d.contentType} to ${storage.id}")
                stored = (storage.store(d) || stored)
            }
        }

        if (!justStore) {
            addToGraphStore([doc])
            addToIndex([doc])
        }

        if (!stored) {
            throw new WhelkAddException("No suitable storage found for content-type ${doc.contentType}.", [doc.identifier])
        }
        return new URI(doc.identifier)
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkAdd(final List<Document> docs) {
        for (doc in docs) {
            add(doc, true)
        }
        addToGraphStore(docs)
        addToIndex(docs)
    }

    @Override
    Document get(URI uri) {
        return storages.get(0)?.get(uri)
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

    Document sanityCheck(Document d) {
        if (!d.identifier) {
            d.identifier = mintIdentifier(d)
        }
        d.timestamp = new Date().getTime()
        return d
    }

    @groovy.transform.CompileStatic
    void addToIndex(List<Document> docs) {
        List<IndexDocument> idxDocs = []
        if (indexes.size() > 0) {
            for (doc in docs) {
                for (ifc in getIndexFormatConverters()) {
                    log.trace("Running indexformatconverter $ifc")
                    idxDocs.addAll(ifc.convert(doc))
                }
            }
            if (idxDocs) {
                for (idx in indexes) {
                    idx.bulkIndex(idxDocs)
                }
            } else if (log.isDebugEnabled()) {
                log.debug("No documents to index.")
            }
        }
    }

    void addToGraphStore(List<Document> docs) {
        if (graphStores.size() > 0) {
            log.debug("Adding to graph stores")
            List<Document> dataDocs = []
            for (doc in docs) {
                for (rc in getRDFFormatConverters()) {
                    log.trace("Running indexformatconverter $rc")
                    dataDocs.addAll(rc.convert(doc))
                }
            }
            if (dataDocs) {
                for (store in graphStores) {
                    dataDocs.each {
                        store.update(docBaseUri.resolve(it.identifier), it)
                    }
                }
            } else (isDebugEnabled()) {
                log.debug("No graphs to update.")
            }
        }
    }

    private List<RDFDescription> convertToRDFDescriptions(List<Document> docs) {
        def rdocs = []
        for (doc in docs) {
            rdocs << new RDFDescription(doc)
        }
        return rdocs
    }

    @Override
    Iterable<Document> loadAll(Date since) { return loadAll(null, since)}

    @Override
    Iterable<Document> loadAll(String dataset = null, Date since = null) {
        def storage = getStorages()[0]
        log.debug("Loading all from storage ${storage.id}")
        return storage.getAll(dataset)
    }

    @Override
    @groovy.transform.CompileStatic
    Document createDocument(byte[] data, Map entrydata, Map<String,Object> metadata=null, boolean convert=true) { return createDocument(new String(data, "UTF-8"), entrydata, metadata, convert) }
    @groovy.transform.CompileStatic
    Document createDocument(String data, Map<String,Object> entrydata, Map<String,Object> metadata=null, boolean convert=true) {
        log.debug("Creating document")
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
        log.trace("Creation complete for ${doc.identifier} (${doc.contentType})")
        if (convert) {
            log.trace("Executing storage format conversion.")
            for (fc in formatConverters) { doc = fc.convert(doc) }
            log.trace("Document ${doc.identifier} has undergone formatconversion.")
        }
        for (lf in linkFinders) {
            for (link in lf.findLinks(doc)) {
                doc = doc.withLink(link.identifier.toString(), link.type)
            }
        }
        log.debug("Returning document ${doc.identifier} (${doc.contentType})")
        return doc
    }

    @Override
    void reindex(String dataset = null, String startAt = null) {
        int counter = 0
        long startTime = System.currentTimeMillis()
        List<Document> docs = []
        boolean indexing = !startAt
        if (dataset) {
            log.debug("Requesting new index.")
            for (index in indexes) {
                index.createNewCurrentIndex()
            }
        }
            for (doc in loadAll(dataset)) {
                if (startAt && doc.identifier == startAt) {
                    log.info("Found document with identifier ${startAt}. Starting to index ...")
                    indexing = true
                }
                if (indexing) {
                    log.trace("Adding doc ${doc.identifier} with type ${doc.contentType}")
                    docs << doc
                    if (++counter % 1000 == 0) { // Bulk index 1000 docs at a time
                        addToGraphStore(docs)
                        try {
                            addToIndex(docs)
                        } catch (WhelkAddException wae) {
                            log.info("Failed indexing identifiers: ${wae.failedIdentifiers}")
                        }
                        docs = []
                        if (log.isInfoEnabled()) {
                            Tools.printSpinner("Reindexing ${this.id}. ${counter} documents sofar.", counter)
                        }
                    }
                }
            }
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            addToGraphStore(docs)
            addToIndex(docs)
        }
        log.info("Reindexed $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
        if (dataset) {
            for (index in indexes) {
                index.reMapAliases()
            }
        }
    }

    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.id}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.id)
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
    List<Storage> getStorages(String rct) { return plugins.findAll { it instanceof Storage && it.requiredContentType == rct} }
    List<Index> getIndexes() { return plugins.findAll { it instanceof Index } }
    List<GraphStore> getGraphStores() { return plugins.findAll { it instanceof GraphStore } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }
    List<FormatConverter> getFormatConverters() { return plugins.findAll { it instanceof FormatConverter }}
    List<IndexFormatConverter> getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter }}
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}

    @Deprecated
    URI oldadd(Document doc) {
        boolean stored = false
        doc = sanityCheck(doc)

        for (storage in storages) {
            stored = (storage.store(doc) || stored)
        }

        addToGraphStore([doc])
        addToIndex([doc])

        if (!stored) {
            throw new WhelkAddException("No suitable storage found for content-type ${doc.contentType}.", [doc.identifier])
        }
        return new URI(doc.identifier)
    }
}

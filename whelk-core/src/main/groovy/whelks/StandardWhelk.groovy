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
    URI add(Document doc) {
        boolean stored = false
        doc = sanityCheck(doc)

        for (storage in storages) {
            stored = (storage.store(doc, this.id) || stored)
        }

        addToGraphStore([doc])
        addToIndex([doc])

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
        boolean stored = false
        for (storage in storages) {
            for (doc in docs) {
                try {
                    stored = (storage.store(doc, this.id) || stored)
                } catch (Exception e) {
                    log.error("Store failed for $doc", e)
                    throw new WhelkAddException(doc.identifier as String)
                }
            }
        }
        addToGraphStore(docs)
        addToIndex(docs)

        if (!stored) {
            throw new WhelkAddException("No suitable storage found for content-type ${docs[0]?.contentType}.", docs*.identifier)
        }
    }

    @Override
    Document get(URI uri) {
        return storages.get(0)?.get(uri, this.id)
    }

    @Override
    void remove(URI uri) {
        components.each {
            try {
                ((Component)it).delete(uri, this.id)
            } catch (RuntimeException rte) {
                log.warn("Component ${((Component)it).id} failed delete: ${rte.message}")
            }
        }
    }

    @Override
    SearchResult search(Query query) {
        return indexes.get(0)?.query(query, this.id)
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
                    idx.bulkIndex(idxDocs, this.id)
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
    Iterable<Document> loadAll(String fromStorage = null, Date since = null) {
        def storage = (fromStorage == null ? getStorages()[0] : getStorages().find { it.id == fromStorage })
        log.debug("Loading all from storage ${storage.id}")
        return storage.getAll(this.id)
    }

    @Override
    @groovy.transform.CompileStatic
    Document createDocument(byte[] data, Map entrydata, Map<String,Object> metadata=null, boolean convert=true) { return createDocument(new String(data, "UTF-8"), entrydata, metadata, convert) }
    @groovy.transform.CompileStatic
    Document createDocument(String data, Map<String,Object> entrydata, Map<String,Object> metadata=null, boolean convert=true) {
        log.debug("Creating document")
        Document doc = new Document().withData(data).withEntry(entrydata).withMeta(metadata)
        /*
        metadata.each { param, value ->
            if (value) {
                doc.metaClass.pickMethod("set${((String)param).capitalize()}",
                    value.getClass()).doMethodInvoke(doc, value)
            }
        }
        */
        log.trace("Creation complete for ${doc.identifier} (${doc.contentType})")
        if (convert) {
            log.trace("Executing storage format conversion.")
            doc = performStorageFormatConversion(doc)
            log.trace("Document ${doc.identifier} has undergone formatconversion.")
        }
        if (doc) {
            for (lf in linkFinders) {
                for (link in lf.findLinks(doc)) {
                    doc = doc.withLink(link.identifier.toString(), link.type)
                }
            }
        }
        log.debug("Returning document ${doc.identifier} (${doc.contentType})")
        return doc
    }

    @groovy.transform.CompileStatic
    Document performStorageFormatConversion(Document doc) {
        for (fc in formatConverters) {
            log.trace("Running formatconverter $fc")
            doc = fc.convert(doc)
        }
        return doc
    }

    @Override
    void reindex(String fromStorage = null, String startAt = null) {
        int counter = 0
        long startTime = System.currentTimeMillis()
        List<Document> docs = []
        boolean indexing = !startAt
        for (doc in loadAll(fromStorage)) {
            if (startAt && doc.identifier == startAt) {
                log.info("Found document with identifier ${startAt}. Starting to index ...")
                indexing = true
            }
            if (indexing) {
                docs << doc
                if (++counter % 1000 == 0) { // Bulk index 1000 docs at a time
                    addToGraphStore(docs)
                    addToIndex(docs)
                    docs = []
                    if (log.isInfoEnabled()) {
                        Tools.printSpinner("Reindexing ${this.id}. ${counter} documents sofar.", counter)
                    }
                }
            }
        }
        if (docs.size() > 0) {
            addToGraphStore(docs)
            addToIndex(docs)
        }
        log.info("Reindexed $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
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
    List<Index> getIndexes() { return plugins.findAll { it instanceof Index } }
    List<GraphStore> getGraphStores() { return plugins.findAll { it instanceof GraphStore } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }
    List<FormatConverter> getFormatConverters() { return plugins.findAll { it instanceof FormatConverter }}
    List<IndexFormatConverter> getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter }}
    List<RDFFormatConverter> getRDFFormatConverters() { return plugins.findAll { it instanceof RDFFormatConverter }}
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}
    List<URIMinter> getUriMinters() { return plugins.findAll { it instanceof URIMinter }}
}

package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import java.util.UUID
import java.net.URI
import java.net.URISyntaxException
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.WhelkRuntimeException
import se.kb.libris.whelks.plugin.*

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

        addToIndex([doc])
        addToQuadStore(doc)

        return doc.identifier
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    @groovy.transform.CompileStatic
    void bulkStore(List<Document> docs) {
        for (storage in storages) {
            for (doc in docs) {
                storage.store(doc, this.prefix)
            }
        }
        addToIndex(docs)
        addToQuadStore(docs)
    }

    @Override
    Document get(URI uri) {
        return storages.get(0)?.get(uri, this.prefix)
    }

    @Override
    void delete(URI uri) {
        components.each {
            try {
                ((Component)it).delete(uri, this.prefix)
            } catch (RuntimeException rte) {
                log.warn("Component ${((Component)it).id} failed delete: ${rte.message}")
            }
        }
    }

    @Override
    SearchResult<? extends Document> query(Query query) {
        return indexes.get(0)?.query(query, this.prefix)
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

    @groovy.transform.CompileStatic
    void addToIndex(List<Document> docs) {
        if (indexes.size() > 0) {
            log.debug("Adding to indexes")
            for (ifc in getIndexFormatConverters()) {
                log.debug("Calling indexformatconverter $ifc")
                    if (docs.size() > 0) {
                        docs = ifc.convertBulk(docs)
                    } else {
                        docs = ifc.convert(docs.get(0))
                    }
            }
            for (idx in indexes) {
                idx.index(docs, this.prefix)
            }
        }
    }

    void addToQuadStore(doc) {}

    @Override
    List<Document> loadAll(Date since = null) {
        throw new UnsupportedOperationException("Not implemented yet")
    }

    @Override
    @groovy.transform.CompileStatic
    Document createDocument(byte[] data, Map metadata) { return createDocument(new String(data), metadata) }
    @groovy.transform.CompileStatic
    Document createDocument(String data, Map<String,Object> metadata) {
        log.debug("Creating document")
        Document doc = new BasicDocument().withData(data)
        metadata.each { param, value ->
            if (value) {
                doc.metaClass.pickMethod("with${((String)param).capitalize()}",
                        value.getClass()).doMethodInvoke(doc, value)
            }
        }
        doc = performStorageFormatConversion(doc)
        for (lf in linkFinders) {
            for (link in lf.findLinks(doc)) {
                doc = doc.withLink(link)
            }
        }
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
    void reindex() {
        int counter = 0
        long startTime = System.currentTimeMillis()
        List<Document> docs = []
        for (doc in loadAll()) {
            for (ifc in indexFormatConverters) {
                doc = ifc.convert(doc)
            }
            docs << doc
            if (++counter % 1000 == 0) { // Bulk index 1000 docs at a time
                for (index in indexes) {
                    index.index(docs, this.prefix)
                    docs = []
                }
            }
        }
        if (docs.size() > 0) {
            for (index in indexes) {
                index.index(docs, this.prefix)
            }
        }
        log.info("Reindexed $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
    }

    @Override
    void addPlugin(Plugin plugin) {
        log.debug("[${this.prefix}] Initializing ${plugin.id}")
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        plugin.init(this.prefix)
        this.plugins.add(plugin)
    }

   URI mintIdentifier(Document d) {
        try {
            return new URI("/"+prefix.toString() +"/"+ UUID.randomUUID());
        } catch (URISyntaxException ex) {
            throw new WhelkRuntimeException("Could not mint URI", ex);
        }
    }

    // Sugar methods
    List<Component> getComponents() { return plugins.findAll { it instanceof Component } }
    List<Storage> getStorages() { return plugins.findAll { it instanceof Storage } }
    List<Index> getIndexes() { return plugins.findAll { it instanceof Index } }
    List<API> getAPIs() { return plugins.findAll { it instanceof API } }
    TreeSet<FormatConverter> getFormatConverters() { return plugins.findAll { it instanceof FormatConverter } as TreeSet}
    TreeSet<IndexFormatConverter> getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter } as TreeSet }
    List<LinkFinder> getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}

}

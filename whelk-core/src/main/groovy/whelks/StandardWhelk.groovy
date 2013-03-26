package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

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

        addToIndex(doc)
        addToQuadStore(doc)

        return doc.identifier
    }

    /**
     * Requires that all documents have an identifier.
     */
    @Override
    void bulkStore(Iterable<Document> docs) {
        for (storage in storages) {
            for (doc in docs) {
                storage.store(doc, this.prefix)
            }
        }
        addToIndex(docs)
        addToQuadStore(doc)
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
        if (indexes.size() > 0) {
            log.debug("Adding to indexes")
            def docs
            if (doc instanceof Document) {
                docs = [doc]
            } else {
                docs = doc
            }
            for (ifc in getIndexFormatConverters()) {
                log.trace("Calling indexformatconverter $ifc")
                docs = ifc.convertBulk(docs)
            }
            for (idx in indexes) {
                idx.index(docs, this.prefix)
            }
        }
    }

    void addToQuadStore(doc) {}

    @Override
    Iterable<Document> loadAll(Date since = null) {
        throw new UnsupportedOperationException("Not implemented yet")
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
    void reindex() {
        int counter = 0
        long startTime = System.currentTimeMillis()
        def docs = []
        for (doc in loadAll()) {
            for (ifc in indexFormatConverters) {
                doc = ifc.convert(doc)
            }
            docs << doc
            if (++counter % 1000 == 0) { // Bulk index 1000 docs at a time
                for (index in indexes) {
                    index.index(docs)
                    docs = []
                }
            }
        }
        if (docs.size() > 0) {
            for (index in indexes) {
                index.index(docs)
            }
        }
        log.info("Reindexed $counter documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds.")
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

    // Sugar methods
    def getComponents() { return plugins.findAll { it instanceof Component } }
    def getStorages() { return plugins.findAll { it instanceof Storage } }
    def getIndexes() { return plugins.findAll { it instanceof Index } }
    def getAPIs() { return plugins.findAll { it instanceof API } }
    def getFormatConverters() { return plugins.findAll { it instanceof FormatConverter } as TreeSet}
    def getIndexFormatConverters() { return plugins.findAll { it instanceof IndexFormatConverter } as TreeSet }
    def getLinkFinders() { return plugins.findAll { it instanceof LinkFinder }}

}

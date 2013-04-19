package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.WhelkRuntimeException

@Log
class CombinedWhelk extends StandardWhelk {

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
            return storages.find { it.id == rules.preferredRetrieveFrom }?.get(uri, this.id)
        }
    }

    @Override
    URI store(Document doc) {
        if (!rules.storeByFormat) {
            log.trace("No direction rules for storing. Using superclass store().")
            return super.store(doc)
        } else {
            doc = sanityCheck(doc)

            addToStorage(doc)

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

    void addToStorage(doc) {
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

        try {
            doc = performStorageFormatConversion(doc)
            idocs << doc
        } catch (Exception e) {
            log.error("Trapped exception ${e.message} for document ${doc.dataAsString}. THIS IS A TEMPORARY SOLUTION AND SHOULD BE REMOVED ASAP! (Blame Markus)", e)
            ok = false
        }

        if (ok) {
            for (storage in remainingStorages) {
                if (!rules.storeByFormat[doc.format] || rules.storeByFormat[doc.format] == storage.id) {
                    log.debug("Storing document with format ${doc.format} in storage with id ${storage.id}")
                    storage.store(doc, this.pfxs)
                }
            }
        }
    }


    @Override
    void bulkStore(Iterable<Document> docs) {
        def idocs = []
        long startTime = System.nanoTime()
        for (doc in docs) {
            addToStorage(doc)
        }
        long meanTime = System.nanoTime()
        log.trace("[bulkStore] Time to store: " + ((meanTime - startTime)/1000000))
        addToIndex(idocs)
        log.trace("[bulkStore] Time to index: " + ((System.nanoTime() - meanTime)/1000000))
        addToQuadStore(idocs)
    }

    SearchResult query(Query q, String indexType) {
        log.trace("query intercepted: $q, $indexType")
        def eq = new ElasticQuery(q)
        eq.indexType = indexType
        return plugins.find { it instanceof Index }?.query(eq, this.pfxs)
    }

    // Maintenance
    void rebuild(from) {
        def sourceStorage = storages.find { it.id == from }
        if (!sourceStorage) {
            throw new WhelkRuntimeException("Cannot rebuild from storage $from. No such storage found.")
        }
        int counter = 0
        for (doc in sourceStorage.getAll(this.id)) {
            if (++counter % 1000 == 0) {
                log.info("Rebuilt $counter records.")
            }
            this.store(doc)
        }
        log.info("Rebuilt $counter records.")
    }

    @Override
    void addPlugin(Plugin plugin) {
        if (plugin instanceof WhelkAware) {
            plugin.setWhelk(this)
        }
        if (rules.dontInitialize?.contains(plugin.id)) {
            log.trace("[${this.id}] Not initializing plugin ${plugin.id}. The rules says so.")
        } else {
            log.trace("[${this.id}] Initializing ${plugin.id}")
            plugin.init(this.id)
        }
        this.plugins.add(plugin)
    }
}

package whelk

import groovy.util.logging.Log4j2 as Log
import org.apache.commons.collections4.map.LRUMap
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.filter.JsonLdLinkExpander
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    ElasticSearch elastic
    JsonLdLinkExpander expander
    Map displayData
    Map vocabData
    JsonLd jsonld

    String vocabDisplayUri = "https://id.kb.se/vocab/display" // TODO: encapsulate and configure (LXL-260)
    String vocabUri = "https://id.kb.se/vocab/" // TODO: encapsulate and configure (LXL-260)

    private DocumentCache docCache
    private static final int CACHE_MAX_SIZE = 1000
    private static final int CACHE_LIFETIME_MILLIS = 30000

    public Whelk(PostgreSQLComponent pg, ElasticSearch es) {
        this.storage = pg
        this.elastic = es
        this.docCache = new DocumentCache()
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(PostgreSQLComponent pg) {
        this.storage = pg
        this.docCache = new DocumentCache()
        log.info("Whelk started with storage $storage")
    }

    public Whelk() {
        this.docCache = new DocumentCache()
    }

    private class DocumentCache {
        private Map cache
        private int staleCount
        private int cacheHits

        private class Entry {
            Document doc
            long lastAccessed

            Entry(Document doc) {
                this.doc = doc
                this.lastAccessed = System.currentTimeMillis()
            }
        }

        DocumentCache() {
            this.cache = Collections.synchronizedMap(
                new LRUMap<String, Entry>(CACHE_MAX_SIZE))
            this.staleCount = 0
            this.cacheHits = 0
        }

        Document get(String key) {
            synchronized(cache) {
                Entry entry = cache[key]
                if (entry) {
                    cacheHits += 1
                    if(isStale(entry)) {
                        cache.remove(key)
                        staleCount += 1
                        return null
                    } else {
                        return entry.doc
                    }
                } else {
                    return null
                }
            }
        }

        void put(String key, Document doc) {
            synchronized(cache) {
                cache[key] = new Entry(doc)
            }
        }

        int getSize() {
            synchronized(cache) {
                return cache.size()
            }
        }

        int getStaleCount() {
            return staleCount
        }

        int getCacheHits() {
            return cacheHits
        }

        private boolean isStale(Entry entry) {
            return (entry.lastAccessed + CACHE_LIFETIME_MILLIS) <
                   System.currentTimeMillis()
        }
    }

    int cacheSize() {
        return docCache.getSize()
    }

    int cacheStaleCount() {
        return docCache.getStaleCount()
    }

    int cacheHits() {
        return docCache.getCacheHits()
    }

    public static DefaultPicoContainer getPreparedComponentsContainer(Properties properties) {
        DefaultPicoContainer pico = new DefaultPicoContainer(new PropertiesPicoContainer(properties))
        Properties componentProperties = PropertyLoader.loadProperties("component")
        for (comProp in componentProperties) {
            if (comProp.key.endsWith("Class") && comProp.value && comProp.value != "null") {
                log.info("Adding pico component ${comProp.key} = ${comProp.value}")
                pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Class.forName(comProp.value))
            }
        }
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Whelk.class)
        return pico
    }

    void loadCoreData() {
        loadDisplayData()
        loadVocabData()
        jsonld = new JsonLd(displayData, vocabData)
    }

    void loadDisplayData() {
        this.displayData = this.storage.locate(vocabDisplayUri, true).document.data
    }

    void loadVocabData() {
        this.vocabData = this.storage.locate(vocabUri, true).document.data
    }

    Map<String, Document> bulkLoad(List ids, boolean useDocumentCache = false) {
        Map result = [:]
        ids.each { id ->
            Document doc
            Document cached = docCache.get(id)
            if (useDocumentCache && cached) {
                result[id] = cached
            } else {
                if (id.startsWith(Document.BASE_URI.toString())) {
                    id = Document.BASE_URI.resolve(id).getPath().substring(1)
                    doc = storage.load(id)
                } else {
                    doc = storage.locate(id, true)?.document
                }

                if (doc && !doc.deleted) {
                    result[id] = doc
                    docCache.put(id, doc)
                }
            }
        }
        return result
    }

    public void reindexDependers(Document document) {
        List<String> dependingIDs = storage.getDependers(document.getShortId())

        // If the number of dependers isn't too large. Update them synchronously
        if (dependingIDs.size() < 20) {
            Map dependingDocuments = bulkLoad(dependingIDs)
            for (String id : dependingDocuments.keySet()) {
                Document dependingDoc = dependingDocuments.get(id)
                String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
                elastic.index(dependingDoc, dependingDocCollection, this)
            }
        } else {
            // else use a fire-and-forget thread
            Whelk _this = this
            new Thread(new Runnable() {
                void run() {
                    for (String id : dependingIDs) {
                        Document dependingDoc = storage.load(id)
                        String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
                        if (dependingDocCollection != null)
                            elastic.index(dependingDoc, dependingDocCollection, _this)
                    }
                }
            }).start()

        }
    }

    /**
     * NEVER use this to _update_ a document. Use storeAtomicUpdate() instead. Using this for new documents is fine.
     */
    Document store(Document document, String changedIn, String changedBy, String collection, boolean deleted, boolean createOrUpdate = true) {
        if (storage.store(document, createOrUpdate, changedIn, changedBy, collection, deleted)) {
            if (elastic) {
                elastic.index(document, collection, this)
                reindexDependers(document)
            }
        }
        return document
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, String collection, boolean deleted, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, collection, deleted, updateAgent)
        if (elastic) {
            elastic.index(updated, collection, this)
            reindexDependers(updated)
        }
        return updated
    }

    void bulkStore(final List<Document> documents, String changedIn,
                   String changedBy, String collection,
                   boolean createOrUpdate = true, boolean useDocumentCache = false) {
        if (storage.bulkStore(documents, createOrUpdate, changedIn, changedBy, collection)) {
            if (elastic) {
                elastic.bulkIndex(documents, collection, this, useDocumentCache)
                for (Document doc : documents) {
                    reindexDependers(doc)
                }
            }
        } else {
            log.warn("Bulk store failed, not indexing : ${documents.first().id} - ${documents.last().id}")
        }
    }

    void remove(String id, String changedIn, String changedBy, String collection) {
        log.debug "Deleting ${id} from Whelk"
        if (storage.remove(id, changedIn, changedBy, collection)) {
            if (elastic) {
                elastic.remove(id)
                log.debug "Object ${id} was removed from Whelk"
            }
            else {
                log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
            }
        } else {
            log.warn "storage did not remove ${id} from whelk"
        }
    }
}

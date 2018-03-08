package whelk

import groovy.util.logging.Log4j2 as Log
import org.apache.commons.collections4.map.LRUMap
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.util.LegacyIntegrationTools

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    ElasticSearch elastic
    Map displayData
    Map vocabData
    JsonLd jsonld

    // useAuthCache may be set to true only when doing initial imports (temporary processes with the rest of Libris down).
    // Any other use of this results in a "local" cache, which will not be invalidated when data changes elsewhere,
    // resulting in potential serving of stale data.
    boolean useAuthCache = false

    String vocabDisplayUri = "https://id.kb.se/vocab/display" // TODO: encapsulate and configure (LXL-260)
    String vocabUri = "https://id.kb.se/vocab/" // TODO: encapsulate and configure (LXL-260)

    private Map<String, Document> authCache
    private final int CACHE_MAX_SIZE = 1000

    static {

    }

    void putInAuthCache(Document authDocument) {
        if (!useAuthCache)
            return
        if (authDocument == null) {
            log.warn("Tried to cache a null document (ignored)!")
            return
        }
        for (String uri : authDocument.getRecordIdentifiers()) {
            authCache.put(uri, authDocument)
        }
        for (String uri : authDocument.getThingIdentifiers()) {
            authCache.put(uri, authDocument)
        }
        authCache.put(authDocument.getShortId(), authDocument)
    }

    public Whelk(PostgreSQLComponent pg, ElasticSearch es, boolean useCache = false) {
        this.storage = pg
        this.elastic = es
        this.useAuthCache = useCache
        if (useCache)
            authCache = Collections.synchronizedMap(
                new LRUMap<String, Document>(CACHE_MAX_SIZE))
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(PostgreSQLComponent pg, boolean useCache = false) {
        this.storage = pg
        this.useAuthCache = useCache
        if (useCache)
            authCache = Collections.synchronizedMap(
                    new LRUMap<String, Document>(CACHE_MAX_SIZE))
        log.info("Whelk started with storage $storage")
    }

    public Whelk(Properties properties, boolean useCache = false) {
        this.storage = new PostgreSQLComponent(properties)
        this.elastic = new ElasticSearch(properties)
        this.useAuthCache = useCache
        if (useCache)
            authCache = Collections.synchronizedMap(
                    new LRUMap<String, Document>(CACHE_MAX_SIZE))
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk() {
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

    //private long hits = 0
    //private long misses = 0

    Map<String, Document> bulkLoad(List ids) {
        Map result = [:]
        ids.each { id ->

            Document doc

            // Check the auth cache
            if (useAuthCache && authCache.containsKey(id)) {
                /*if (hits++ % 100 == 0)
                    println("Fetching with cache: $id, Sofar hits: $hits , misses: $misses")*/
                result[id] = authCache.get(id)
            } else {

                // Fetch from DB
                if (id.startsWith(Document.BASE_URI.toString())) {
                    id = Document.BASE_URI.resolve(id).getPath().substring(1)
                    doc = storage.load(id)
                } else {
                    doc = storage.locate(id, true)?.document
                }

                if (doc && !doc.deleted) {

                    /*if (misses++ % 100 == 0)
                        println("Fetching without cache: $id (${doc.getCompleteId()}), Sofar hits: $hits , misses: $misses")*/

                    result[id] = doc
                    String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                    if (collection == "auth" || collection == "definitions"
                            || collection == null) // TODO: Remove ASAP when mainEntity,@type mappings correctly map to collection again.
                        putInAuthCache(doc)
                }
            }
        }
        return result
    }

    public void reindexDependers(Document document) {
        List<Tuple2<String, String>> dependers = storage.getDependers(document.getShortId())

        // Filter out "itemOf"-links. In other words, do not bother reindexing hold posts (they're not embellished in elastic)
        List<String> idsToReindex = []
        for (Tuple2<String, String> depender : dependers) {
            if (!depender.get(1).equals("itemOf")) {
                idsToReindex.add( (String) depender.get(0))
            }
        }

        // If the number of dependers isn't too large. Update them synchronously
        if (dependers.size() < 20) {
            Map dependingDocuments = bulkLoad(idsToReindex)
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
                    for (String id : idsToReindex) {
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
     * Returns tuples for ID collisions, the first entry in the tuple is the system ID of the colliding record,
     * the second is a freetext description of the reason for the collision
     */
    List<Tuple2<String, String>> getIdCollisions(Document document, boolean includingTypedIDs) {

        List<Tuple2<String, String>> collidingSystemIDs = []

        // Identifiers-table lookup on:
        List<String> uriIDs = document.getRecordIdentifiers()
        uriIDs.addAll( document.getThingIdentifiers() )
        for (String uriID : uriIDs) {
            String systemId = storage.getSystemIdByIri(uriID)
            if (systemId != null && systemId != document.getShortId()) {
                log.info("Determined that " + document.getShortId() + " is duplicate of " + systemId + " due to collision on URI: " + uriID)
                collidingSystemIDs.add( new Tuple2(systemId, "on URI: " + uriID) )
            }
        }

        // Typed id queries on:
        List<Tuple> typedIDs = document.getTypedRecordIdentifiers()
        typedIDs.addAll( document.getTypedThingIdentifiers() )
        for (Tuple typedID : typedIDs) {
            String type = typedID[0]
            String value = typedID[1]
            int graphIndex = typedID[2].intValue()
            
            // "Identifier" and "SystemNumber" are too general/meaningless to use for duplication checking.
            if (type.equals("Identifier") || type.equals("SystemNumber"))
                continue

            List<String> collisions = storage.getSystemIDsByTypedID(type, value, graphIndex)
            if (!collisions.isEmpty()) {
                if (includingTypedIDs) {
                    for (String collision : collisions) {
                        if (collision != document.getShortId())
                        collidingSystemIDs.add( new Tuple2(collision, "on typed id: " + type + "," + graphIndex + "," + value) )
                    }
                } else {

                    // We currently are not allowed to enforce typed identifier uniqueness. :(
                    // We can warn at least.
                    log.warn("While testing " + document.getShortId() + " for collisions: Ignoring typed ID collision with : "
                            + collisions + " on " + type + "," + graphIndex + "," + value + " for political reasons.")
                }
            }
        }

        return collidingSystemIDs
    }

    /**
     * NEVER use this to _update_ a document. Use storeAtomicUpdate() instead. Using this for new documents is fine.
     */
    Document createDocument(Document document, String changedIn, String changedBy, String collection, boolean deleted) {

        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new whelk.exception.StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
        }

        if (storage.createDocument(document, changedIn, changedBy, collection, deleted)) {
            if (collection == "auth" || collection == "definitions")
                putInAuthCache(document)
            if (elastic) {
                elastic.index(document, collection, this)
                reindexDependers(document)
            }
        }
        return document
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, updateAgent)
        String collection = LegacyIntegrationTools.determineLegacyCollection(updated, jsonld)
        if (collection == "auth" || collection == "definitions")
            putInAuthCache(updated)
        if (elastic) {
            elastic.index(updated, collection, this)
            reindexDependers(updated)
        }
        return updated
    }

    void bulkStore(final List<Document> documents, String changedIn,
                   String changedBy, String collection, boolean useDocumentCache = false) {
        if (storage.bulkStore(documents, changedIn, changedBy, collection)) {
            for (Document doc : documents) {
                if (collection == "auth" || collection == "definitions") {
                    putInAuthCache(doc)
                }
            }
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

    void remove(String id, String changedIn, String changedBy) {
        log.debug "Deleting ${id} from Whelk"
        Document toBeRemoved = storage.load(id)
        if (storage.remove(id, changedIn, changedBy)) {
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

    void mergeExisting(String remainingID, String disappearingID, Document remainingDocument, String changedIn, String changedBy, String collection) {
        storage.mergeExisting(remainingID, disappearingID, remainingDocument, changedIn, changedBy, collection, jsonld)

        if (elastic) {
            String remainingSystemID = storage.getSystemIdByIri(remainingID)
            String disappearingSystemID = storage.getSystemIdByIri(disappearingID)
            List<Tuple2<String, String>> dependerRows = storage.getDependers(remainingSystemID)
            dependerRows.addAll( storage.getDependers(disappearingSystemID) )
            List<String> dependerSystemIDs = []
            for (Tuple2<String, String> dependerRow : dependerRows) {
                dependerSystemIDs.add( (String) dependerRow.get(0) )
            }
            Map<String, Document> dependerDocuments = bulkLoad(dependerSystemIDs)

            List<Document> authDocs = []
            List<Document> bibDocs = []
            List<Document> holdDocs = []
            for (Object key : dependerDocuments.keySet()) {
                Document doc = dependerDocuments.get(key)
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                if (dependerCollection.equals("auth"))
                    authDocs.add(doc)
                else if (dependerCollection.equals("bib"))
                    bibDocs.add(doc)
                else if (dependerCollection.equals("hold"))
                    holdDocs.add(doc)
            }

            elastic.bulkIndex(authDocs, "auth", this)
            elastic.bulkIndex(bibDocs, "bib", this)
            elastic.bulkIndex(holdDocs, "hold", this)
        }
    }
}

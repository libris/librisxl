package whelk

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.collections4.map.LRUMap
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.exception.StorageCreateFailedException
import whelk.filter.LinkFinder
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

/**
 * The Whelk is the root component of the XL system.
 */
@Log
@CompileStatic
class Whelk implements Storage {
    ThreadGroup indexers = new ThreadGroup("dep-reindex")
    PostgreSQLComponent storage
    ElasticSearch elastic
    Map displayData
    Map vocabData
    Map contextData
    JsonLd jsonld
    MarcFrameConverter marcFrameConverter
    Relations relations

    URI baseUri = null

    // useAuthCache may be set to true only when doing initial imports (temporary processes with the rest of Libris down).
    // Any other use of this results in a "local" cache, which will not be invalidated when data changes elsewhere,
    // resulting in potential serving of stale data.
    boolean useAuthCache = false

    // TODO: encapsulate and configure (LXL-260)
    String vocabContextUri = "https://id.kb.se/vocab/context"
    String vocabDisplayUri = "https://id.kb.se/vocab/display"
    String vocabUri = "https://id.kb.se/vocab/"

    private Map<String, Document> authCache
    private final int CACHE_MAX_SIZE = 200_000

    static Whelk createLoadedCoreWhelk(String propName = "secret", boolean useCache = false) {
        return createLoadedCoreWhelk(PropertyLoader.loadProperties(propName), useCache)
    }

    static Whelk createLoadedCoreWhelk(Properties configuration, boolean useCache = false) {
        PostgreSQLComponent storage = new PostgreSQLComponent(configuration)
        Whelk whelk = new Whelk(storage, useCache)
        if (configuration.baseUri) {
            whelk.baseUri = new URI((String) configuration.baseUri)
        }
        whelk.loadCoreData()
        return whelk
    }

    static Whelk createLoadedSearchWhelk(String propName = "secret", boolean useCache = false) {
        return createLoadedSearchWhelk(PropertyLoader.loadProperties(propName), useCache)
    }

    static Whelk createLoadedSearchWhelk(Properties configuration, boolean useCache = false) {
        Whelk whelk = new Whelk(configuration, useCache)
        if (configuration.baseUri) {
            whelk.baseUri = new URI((String) configuration.baseUri)
        }
        whelk.loadCoreData()
        return whelk
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
        this(pg, useCache)
        this.elastic = es
        log.info("Using index: $elastic")
    }

    public Whelk(PostgreSQLComponent pg, boolean useCache = false) {
        this.storage = pg
        this.useAuthCache = useCache
        relations = new Relations(pg)
        if (useCache)
            authCache = Collections.synchronizedMap(
                    new LRUMap<String, Document>(CACHE_MAX_SIZE))
        log.info("Started with storage: $storage")
    }

    public Whelk(Properties conf, boolean useCache = false) {
        this(new PostgreSQLComponent(conf), new ElasticSearch(conf), useCache)
    }

    public Whelk() {
    }

    synchronized MarcFrameConverter getMarcFrameConverter() {
        if (!marcFrameConverter) {
            marcFrameConverter = new MarcFrameConverter(new LinkFinder(storage), jsonld)
        }

        return marcFrameConverter
    }

    Relations getRelations() {
        return relations
    }

    void loadCoreData() {
        loadContextData()
        loadDisplayData()
        loadVocabData()
        jsonld = new JsonLd(contextData, displayData, vocabData)
        log.info("Loaded with core data")
    }

    void loadContextData() {
        this.contextData = this.storage.getDocumentByIri(vocabContextUri).data
    }

    void loadDisplayData() {
        this.displayData = this.storage.getDocumentByIri(vocabDisplayUri).data
    }

    void loadVocabData() {
        this.vocabData = this.storage.getDocumentByIri(vocabUri).data
    }

    Document getDocument(String id) {
        Document doc = storage.load(id)
        if (baseUri) {
            doc.baseUri = baseUri
        }
        return doc
    }

    Map<String, Document> bulkLoad(List<String> ids) {
        Map<String, Document> result = [:]
        ids.each { id ->
            Document doc

            // Check the auth cache
            if (useAuthCache && authCache.containsKey(id)) {
                result[id] = authCache.get(id)
            } else {
                // Fetch from DB
                if (id.startsWith(Document.BASE_URI.toString())) {
                    id = Document.BASE_URI.resolve(id).getPath().substring(1)
                }
                doc = storage.load(id)
                if (doc == null)
                    doc = storage.getDocumentByIri(id)

                if (doc && !doc.deleted) {
                    result[id] = doc
                    String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                    // TODO: only put used dependencies in cache; and either factor out collectionsToCache, or skip that check!
                    if (collection == "auth" || collection == "definitions"
                            || collection == null) // TODO: Remove ASAP when mainEntity,@type mappings correctly map to collection again.
                        putInAuthCache(doc)
                }
            }
        }
        return result
    }

    private void reindexAffected(Document document, Set<String> preUpdateDependencies) {
        List<Tuple2<String, String>> dependers = storage.followDependers(document.getShortId(), JsonLd.NON_DEPENDANT_RELATIONS)

        // Filter out "itemOf"-links. In other words, do not bother reindexing hold posts (they're not embellished in elastic)
        TreeSet<String> idsToReindex = new TreeSet<>()
        for (Tuple2<String, String> depender : dependers) {
            if (!depender.get(1).equals("itemOf")) {
                idsToReindex.add( (String) depender.get(0))
            }
        }

        Runnable reindex = {
            log.debug("Reindexing ${idsToReindex.size()} affected documents")
            idsToReindex.each { id ->
                Document doc = storage.load(id)
                elastic.index(doc, getLegacyCollection(doc), this)
            }
            updateLinkCount(document, preUpdateDependencies)
        }

        // If the number of dependers isn't too large or we are inside a batch job. Update them synchronously
        if (idsToReindex.size() < 20 || batchJobThread() ) {
            reindex.run()
        } else {
            // else use a fire-and-forget thread
            new Thread(indexers, reindex).start()
        }
    }

    private void updateLinkCount(Document document, Set<String> preUpdateDependencies) {
        Set<String> postUpdateDependencies = storage.getDependencies(document.getShortId())

        (preUpdateDependencies - postUpdateDependencies)
                .collect{ id -> storage.load(id) }
                .each { doc -> elastic.decrementReverseLinks(doc.getShortId(), getLegacyCollection(doc))}

        (postUpdateDependencies - preUpdateDependencies)
                .collect{ id -> storage.load(id) }
                .each { doc -> elastic.incrementReverseLinks(doc.getShortId(), getLegacyCollection(doc))}
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
        typedIDs.addAll(document.getTypedThingIdentifiers())
        for (Tuple typedID : typedIDs) {
            String type = typedID[0]
            String value = typedID[1]
            int graphIndex = ((Integer) typedID[2]).intValue()

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
    boolean createDocument(Document document, String changedIn, String changedBy, String collection, boolean deleted) {

        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
        }

        boolean success = storage.createDocument(document, changedIn, changedBy, collection, deleted)
        if (success) {
            if (collection == "auth" || collection == "definitions")
                putInAuthCache(document)
            if (elastic) {
                elastic.index(document, collection, this)
                reindexAffected(document, new TreeSet<String>())
            }
        }
        return success
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, Storage.UpdateAgent updateAgent) {
        Collection<String> preUpdateDependencies = storage.getDependencies(id)
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, updateAgent)
        if (updated == null) {
            return null
        }
        String collection = LegacyIntegrationTools.determineLegacyCollection(updated, jsonld)
        if (collection == "auth" || collection == "definitions")
            putInAuthCache(updated)
        if (elastic) {
            elastic.index(updated, collection, this)
            reindexAffected(updated, preUpdateDependencies)
        }
        return updated
    }

    /**
     * This is a variant of createDocument that does no or minimal denormalization or indexing.
     * It should NOT be used to create records in a production environment. Its intended purpose is
     * to be used when copying data from one xl environment to another.
     */
    boolean quickCreateDocument(Document document, String changedIn, String changedBy, String collection) {
        return storage.quickCreateDocument(document, changedIn, changedBy, collection)
    }

    void bulkStore(final List<Document> documents, String changedIn,
                   String changedBy, String collection,
                   @Deprecated boolean useDocumentCache = false) {
        if (storage.bulkStore(documents, changedIn, changedBy, collection)) {
            for (Document doc : documents) {
                if (collection == "auth" || collection == "definitions") {
                    putInAuthCache(doc)
                }
            }
            if (elastic) {
                elastic.bulkIndex(documents, collection, this)
                for (Document doc : documents) {
                    reindexAffected(doc, new TreeSet<String>())
                }
            }
        } else {
            log.warn("Bulk store failed, not indexing : ${documents.first().id} - ${documents.last().id}")
        }
    }

    void remove(String id, String changedIn, String changedBy) {
        log.debug "Deleting ${id} from Whelk"
        storage.remove(id, changedIn, changedBy)
        if (elastic) {
            elastic.remove(id)
            log.debug "Object ${id} was removed from Whelk"
        }
        else {
            log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
        }
    }

    void mergeExisting(String remainingID, String disappearingID, Document remainingDocument, String changedIn, String changedBy, String collection) {
        storage.mergeExisting(remainingID, disappearingID, remainingDocument, changedIn, changedBy, collection, jsonld)

        if (elastic) {
            String remainingSystemID = storage.getSystemIdByIri(remainingID)
            String disappearingSystemID = storage.getSystemIdByIri(disappearingID)
            List<Tuple2<String, String>> dependerRows = storage.followDependers(remainingSystemID, JsonLd.NON_DEPENDANT_RELATIONS)
            dependerRows.addAll( storage.followDependers(disappearingSystemID) )
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

    void embellish(Document document, boolean filterOutNonChipTerms = true) {
        storage.embellish(document, jsonld, filterOutNonChipTerms, this.&bulkLoad)
    }

    Document loadEmbellished(String systemId) {
        return storage.loadEmbellished(systemId, jsonld)
    }

    List<String> findIdsLinkingTo(String idOrIri) {
        return storage
                .getDependers(tryGetSystemId(idOrIri))
                .collect()
    }

    private String tryGetSystemId(String id) {
        String systemId = storage.getSystemIdByThingId(id)
        if (systemId == null) {
            systemId = stripBaseUri(id)
        }
        return systemId
    }

    private String getLegacyCollection(Document doc) {
        return LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                ?: storage.getCollectionBySystemID(doc.getShortId())
    }

    private static String stripBaseUri(String identifier) {
        return identifier.startsWith(Document.BASE_URI.toString())
                ? identifier.substring(Document.BASE_URI.toString().length())
                : identifier
    }

    private boolean batchJobThread() {
        return Thread.currentThread().getThreadGroup().getName().contains("whelktool")
    }
}

package whelk

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableFutureTask
import com.google.common.util.concurrent.ThreadFactoryBuilder
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

import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

/**
 * The Whelk is the root component of the XL system.
 */
@Log
@CompileStatic
class Whelk implements Storage {
    private static final List<String> BROADER_RELATIONS = ['broader', 'broadMatch', 'exactMatch']

    ThreadGroup indexers = new ThreadGroup("dep-reindex")
    PostgreSQLComponent storage
    ElasticSearch elastic
    Map displayData
    Map vocabData
    Map contextData
    JsonLd jsonld
    MarcFrameConverter marcFrameConverter

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

    private Executor cacheRefresher = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build())

    private LoadingCache<String, List<String>> broaderCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<String, List<String>>() {
                @Override
                List<String> load(String id) {
                    return computeInverseBroaderRelations(id)
                }

                @Override
                ListenableFuture<List<String>> reload(String id, List<String> oldValue) throws Exception {
                    return reloadTask( { load(id) } )
                }
            })

    private LoadingCache<Tuple2<String,String>, List<String>> dependencyCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(5, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<Tuple2<String,String>, List<String>>() {
                @Override
                List<String> load(Tuple2<String,String> idAndRelation) {
                    String id = idAndRelation.first
                    String typeOfRelation = idAndRelation.second
                    return storage
                            .getDependenciesOfType(storage.getSystemIdByThingId(id), typeOfRelation)
                            .collect(storage.&getThingMainIriBySystemId)
                }

                @Override
                ListenableFuture<List<String>> reload(Tuple2<String,String> key, List<String> oldValue) throws Exception {
                    return reloadTask( { load(key) } )
                }
            })

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
                }
                doc = storage.load(id)
                if (doc == null)
                    doc = storage.getDocumentByIri(id)


                if (doc && !doc.deleted) {

                    /*if (misses++ % 100 == 0)
                        println("Fetching without cache: $id (${doc.getCompleteId()}), Sofar hits: $hits , misses: $misses")*/

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

    public void reindexDependers(Document document) {
        List<Tuple2<String, String>> dependers = storage.getDependers(document.getShortId())

        // Filter out "itemOf"-links. In other words, do not bother reindexing hold posts (they're not embellished in elastic)
        List<String> idsToReindex = []
        for (Tuple2<String, String> depender : dependers) {
            if (!depender.get(1).equals("itemOf")) {
                idsToReindex.add( (String) depender.get(0))
            }
        }

        // If the number of dependers isn't too large or we are inside a batch job. Update them synchronously
        if (dependers.size() < 20 || batchJobThread() ) {
            Map dependingDocuments = bulkLoad(idsToReindex)
            for (String id : dependingDocuments.keySet()) {
                Document dependingDoc = dependingDocuments.get(id)
                String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
                elastic.index(dependingDoc, dependingDocCollection, this)
            }
        } else {
            // else use a fire-and-forget thread
            Whelk _this = this
            new Thread(indexers, new Runnable() {
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
    Document createDocument(Document document, String changedIn, String changedBy, String collection, boolean deleted) {

        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
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

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, Storage.UpdateAgent updateAgent) {
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, updateAgent)
        if (updated == null) {
            return null
        }
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
                    reindexDependers(doc)
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

    void embellish(Document document, boolean filterOutNonChipTerms = true) {
        List externalRefs = document.getExternalRefs()
        List convertedExternalLinks = JsonLd.expandLinks(externalRefs,
                (Map) jsonld.getDisplayData().get(JsonLd.CONTEXT_KEY))
        Map referencedData = bulkLoad(convertedExternalLinks)
        Map referencedData2 = new HashMap()
        for (Object key : referencedData.keySet()) {
            referencedData2.put(key, ((Document) referencedData.get(key)).data)
        }
        jsonld.embellish(document.data, referencedData2, filterOutNonChipTerms)
    }

    boolean isImpliedBy(String broaderId, String narrowerId) {
        Set<String> visited = []
        List<String> stack = [narrowerId]

        while (!stack.isEmpty()) {
            String id = stack.pop()
            for (String relation : BROADER_RELATIONS) {
                List<String> dependencies = getDependenciesOfType(id, relation)
                if (dependencies.contains(broaderId)) {
                   return true
                }
                dependencies.removeAll(visited)
                visited.addAll(dependencies)
                stack.addAll(dependencies)
            }
        }

        return false
    }



    List<String> findIdsLinkingTo(String id) {
        return storage
                .getDependers(tryGetSystemId(id))
                .collect { it.first }
    }

    List<String> findInverseBroaderRelations(String id) {
        return broaderCache.getUnchecked(id)
    }

    private List<String> computeInverseBroaderRelations(String id) {
        return storage.getNestedDependers(tryGetSystemId(id), BROADER_RELATIONS)
    }

    private List<String> getDependenciesOfType(String id, String typeOfRelation) {
        return dependencyCache.getUnchecked(new Tuple2<>(id, typeOfRelation))
    }

    private String tryGetSystemId(String id) {
        String systemId = storage.getSystemIdByThingId(id)
        if (systemId == null) {
            systemId = stripBaseUri(id)
        }
        return systemId
    }

    private static String stripBaseUri(String identifier) {
        return identifier.startsWith(Document.BASE_URI.toString())
                ? identifier.substring(Document.BASE_URI.toString().length())
                : identifier
    }

    private boolean batchJobThread() {
        return Thread.currentThread().getThreadGroup().getName().contains("whelktool")
    }

    private <V> ListenableFutureTask<V> reloadTask(Supplier<V> reloadFunction) {
        ListenableFutureTask<V> task = ListenableFutureTask.create(new Callable<V>() {
            @Override
            V call() throws Exception {
                return reloadFunction.get()
            }
        })

        cacheRefresher.execute(task)
        return task
    }
}

package whelk

import com.google.common.collect.Iterables
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import groovy.util.logging.Log4j2 as Log
import se.kb.libris.Normalizers
import whelk.component.CachingPostgreSQLComponent
import whelk.component.DocumentNormalizer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.component.SparqlUpdater
import whelk.converter.marc.MarcFrameConverter
import whelk.exception.StorageCreateFailedException
import whelk.external.ExternalEntities
import whelk.filter.LinkFinder
import whelk.filter.NormalizerChain
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import java.time.ZoneId
import java.util.function.Function

/**
 * The Whelk is the root component of the XL system.
 */
@Log
@CompileStatic
class Whelk {
    ThreadGroup indexers = new ThreadGroup("dep-reindex")
    PostgreSQLComponent storage
    ElasticSearch elastic
    SparqlUpdater sparqlUpdater

    Map displayData
    Map vocabData
    Map contextData
    JsonLd jsonld
    MarcFrameConverter marcFrameConverter
    Relations relations
    ExternalEntities external = new ExternalEntities()
    DocumentNormalizer normalizer
    ElasticFind elasticFind
    ZoneId timezone = ZoneId.of('Europe/Stockholm')

    URI baseUri = null
    boolean skipIndex = false

    // useCache may be set to true only when doing initial imports (temporary processes with the rest of Libris down).
    // Any other use of this results in a "local" cache, which will not be invalidated when data changes elsewhere,
    // resulting in potential serving of stale data.

    // TODO: encapsulate and configure (LXL-260)
    String vocabContextUri = "https://id.kb.se/vocab/context"
    String vocabDisplayUri = "https://id.kb.se/vocab/display"
    String vocabUri = "https://id.kb.se/vocab/"

    static Whelk createLoadedCoreWhelk(String propName = "secret", boolean useCache = false) {
        return createLoadedCoreWhelk(PropertyLoader.loadProperties(propName), useCache)
    }

    static Whelk createLoadedCoreWhelk(Properties configuration, boolean useCache = false) {
        Whelk whelk = new Whelk(useCache ? new CachingPostgreSQLComponent(configuration) : new PostgreSQLComponent(configuration))
        whelk.configureAndLoad(configuration)
        return whelk
    }

    static Whelk createLoadedSearchWhelk(String propName = "secret", boolean useCache = false) {
        return createLoadedSearchWhelk(PropertyLoader.loadProperties(propName), useCache)
    }

    static Whelk createLoadedSearchWhelk(Properties configuration, boolean useCache = false) {
        Whelk whelk = new Whelk(configuration, useCache)
        whelk.configureAndLoad(configuration)
        return whelk
    }

    Whelk(PostgreSQLComponent pg, ElasticSearch es) {
        this(pg)
        this.elastic = es
        log.info("Using index: $elastic")
    }

    Whelk(PostgreSQLComponent pg) {
        this.storage = pg
        relations = new Relations(pg)
        log.info("Started with storage: $storage")
    }

    Whelk(Properties conf, useCache = false) {
        this(useCache ? new CachingPostgreSQLComponent(conf) : new PostgreSQLComponent(conf), new ElasticSearch(conf))
    }

    private void configureAndLoad(Properties configuration) {
        if (configuration.baseUri) {
            baseUri = new URI((String) configuration.baseUri)
        }
        if (configuration.timezone) {
            timezone = ZoneId.of((String) configuration.timezone)
        }
        loadCoreData()
        sparqlUpdater = SparqlUpdater.build(storage, jsonld.context, configuration)
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
        setJsonld(new JsonLd(contextData, displayData, vocabData))
        log.info("Loaded with core data")
    }

    void setJsonld(JsonLd jsonld) {
        this.jsonld = jsonld
        storage.setJsonld(jsonld)
        if (elastic) {
            elasticFind = new ElasticFind(new ESQuery(this))
            initDocumentNormalizers()
        }
    }
    
    private void initDocumentNormalizers() {
        normalizer = new NormalizerChain(
                [
                        Normalizers.nullRemover(),
                        //FIXME: This is KBV specific stuff
                        Normalizers.workPosition(jsonld),
                        Normalizers.typeSingularity(jsonld),
                        Normalizers.language(this),
                        Normalizers.identifiedBy(),
                ] + Normalizers.heuristicLinkers(this)
        )
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
        if (doc && baseUri) {
            doc.baseUri = baseUri
        }
        return doc
    }

    Map<String, Document> bulkLoad(Collection<String> ids) {
        Map<String, Document> result = [:]
        ids.each { id ->
            Document doc

            // Fetch from DB
            if (id.startsWith(Document.BASE_URI.toString())) {
                id = Document.BASE_URI.resolve(id).getPath().substring(1)
            }
            doc = storage.load(id)
            if (doc == null)
                doc = storage.getDocumentByIri(id)

            if (doc && !doc.deleted) {
                result[id] = doc
            }
        }
        return result
    }

    private void reindex(Document updated, Document preUpdateDoc) {
        if (elastic && !skipIndex) {
            elastic.index(updated, this)

            if (hasChangedMainEntityId(updated, preUpdateDoc)) {
                reindexAllLinks(updated.shortId)
            } else {
                reindexAffected(updated, preUpdateDoc.getExternalRefs(), updated.getExternalRefs())
            }
        }
    }

    private void reindexAffected(Document document, Set<Link> preUpdateLinks, Set<Link> postUpdateLinks) {
        Runnable reindex = {
            try {
                reindexAffectedSync(document, preUpdateLinks, postUpdateLinks)
            } 
            catch (Exception e) {
                log.error("Error reindexing: $e", e)
            }
        }

        // If we are inside a batch job. Update them synchronously
        if (batchJobThread()) {
            reindex.run()
        } else {
            // else use a fire-and-forget thread
            new Thread(indexers, reindex).start()
        }
    }

    private void reindexAllLinks(String id) {
        SortedSet<String> links = storage.getDependencies(id)
        links.addAll(storage.getDependers(id))
        bulkIndex(links)
    }

    private void reindexAffectedSync(Document document, Set<Link> preUpdateLinks, Set<Link> postUpdateLinks) {
        Set<Link> addedLinks = (postUpdateLinks - preUpdateLinks)
        Set<Link> removedLinks = (preUpdateLinks - postUpdateLinks)
        
        removedLinks.findResults { storage.getSystemIdByIri(it.iri) }
                .each{id -> elastic.decrementReverseLinks(id) }

        addedLinks.each { link ->
            String id = storage.getSystemIdByIri(link.iri)
            if (id) {
                Document doc = storage.load(id)
                def lenses = ['chips', 'cards', 'full']
                def reverseRelations = lenses.collect{ jsonld.getInverseProperties(doc.data, it) }.flatten()
                if (reverseRelations.contains(link.relation)) {
                    // we added a link to a document that includes us in its @reverse relations, reindex it
                    elastic.index(doc, this)
                }
                else {
                    // just update link counter
                    elastic.incrementReverseLinks(id)
                }
            }
        }
        
        if (storage.isCardChangedOrNonexistent(document.getShortId())) {
            bulkIndex(elastic.getAffectedIds(document.getThingIdentifiers() + document.getRecordIdentifiers()))
        }
    }
    
    private void bulkIndex(Iterable<String> ids) {
        Iterables.partition(ids, 100).each {
            elastic.bulkIndexWithRetry(it, this)
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
            if (type == "Identifier" || type == "SystemNumber")
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
        normalize(document)
        
        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
        }

        createCacheRecordsAndPlaceholders(document)
        boolean success = storage.createDocument(document, changedIn, changedBy, collection, deleted)
        if (success) {
            if (elastic && !skipIndex) {
                elastic.index(document, this)
                reindexAffected(document, new TreeSet<>(), document.getExternalRefs())
            }
            sparqlUpdater?.pollNow()
        }
        return success
    }

    /**
     * The UpdateAgent SHOULD be a pure function since the update will be retried in case the document
     * was modified in another transaction.
     */
    void storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document preUpdateDoc = null
        Document updated = storage.storeUpdate(id, minorUpdate, changedIn, changedBy, { Document doc ->
            preUpdateDoc = doc.clone()
            updateAgent.update(doc)
            normalize(doc)
            createCacheRecordsAndPlaceholders(doc, preUpdateDoc)
        })

        if (updated == null || preUpdateDoc == null) {
            return
        }

        reindex(updated, preUpdateDoc)
        sparqlUpdater?.pollNow()
    }

    void storeAtomicUpdate(Document doc, boolean minorUpdate, String changedIn, String changedBy, String oldChecksum) {
        normalize(doc)
        Document preUpdateDoc = storage.load(doc.shortId)
        createCacheRecordsAndPlaceholders(doc, preUpdateDoc)
        Document updated = storage.storeAtomicUpdate(doc, minorUpdate, changedIn, changedBy, oldChecksum)

        if (updated == null) {
            return
        }
        
        reindex(updated, preUpdateDoc)
        sparqlUpdater?.pollNow()
    }
    
    /**
     * This is a variant of createDocument that does no or minimal denormalization or indexing.
     * It should NOT be used to create records in a production environment. Its intended purpose is
     * to be used when copying data from one xl environment to another.
     */
    boolean quickCreateDocument(Document document, String changedIn, String changedBy, String collection) {
        return storage.quickCreateDocument(document, changedIn, changedBy, collection)
    }
  
    void remove(String id, String changedIn, String changedBy) {
        log.debug "Deleting ${id} from Whelk"
        Document doc = storage.load(id)
        storage.remove(id, changedIn, changedBy)
        if (elastic && !skipIndex) {
            elastic.remove(id)
            reindexAffected(doc, doc.getExternalRefs(), new TreeSet<>())
            log.debug "Object ${id} was removed from Whelk"
        }
        else {
            log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
        }
    }

    static boolean hasChangedMainEntityId(Document updated, Document preUpdateDoc) {
        preUpdateDoc.getThingIdentifiers()[0] &&
                updated.getThingIdentifiers()[0] &&
                updated.getThingIdentifiers()[0] != preUpdateDoc.getThingIdentifiers()[0]
    }
    @TypeChecked(TypeCheckingMode.SKIP)
    void embellish(Document document, List<String> levels = null) {
        def getDocs = andGetExternal({ List<String> iris -> bulkLoad(iris).values().collect{ it.data } })
        def getCards = andGetExternal(storage.&getCards, true)
        Embellisher e = new Embellisher(jsonld, getDocs, getCards, relations.&getByReverse)

        if(levels) {
            e.setEmbellishLevels(levels)
        }

        e.embellish(document)
    }
    
    //FIXME
    @TypeChecked(TypeCheckingMode.SKIP)
    private def andGetExternal(Function<Iterable<String>, Iterable<Map>> f, cards = false) {
        def thingId = { graph -> (String) Document._get(Document.thingIdPath, graph) }
        
        return { Iterable<String> iris ->
            def result = f.apply(iris).collect {
                def d = new Document(it) 
                if (d.isPlaceholder()) {
                    external.getEphemeral(d.getThingIdentifiers().first()).ifPresent({ ext ->
                        d.setThing(cards ? jsonld.toCard(ext.getThing(), false) : ext.getThing())
                    })
                    d.data
                } else {
                    it
                }
            }
            
            // get external for IRIs that don't have placeholders
            // TODO: only needed if we don't store placeholders for everything
            def found = result.collect(thingId)
            def missing = ((iris as Set) - (found as Set)) 
            def ext = missing
                    .collect{ external.getEphemeral(it) }
                    .findAll{ it.isPresent() }
                    .collect {cards ? jsonld.toCard(it.get().data) : it.get().data }
            
            result += ext
            
            return result
        }
    }
    
    /**
     * Get cards
     * @param iris
     * @return map from all thing and record identifiers (including sameAs) to corresponding card of whole document 
     */
    Map<String, Map> getCards(Iterable<String> iris) {
        Map<String, Map> result = [:]
        storage.getCards(iris).each { card ->
            List<Map> graph = (List<Map>) card['@graph']
            graph?.each { Map e ->
                e['@id']?.with { result[(String) it] = card }
                if (e.sameAs) {
                    ((List<Map>) (e.sameAs)).each { Map sameAs ->
                        sameAs['@id']?.with { result[(String) it] = card }
                    }
                }
            }
        }
        
        return result
    }

    Document loadEmbellished(String systemId) {
        return storage.loadEmbellished(systemId, this.&embellish)
    }

    List<Document> getAttachedHoldings(List<String> thingIdentifiers) {
        return storage.getAttachedHoldings(thingIdentifiers).collect(this.&loadEmbellished)
    }

    void normalize(Document doc) {
        try {
            doc.normalizeUnicode()
            doc.trimStrings()

            if (normalizer != null) {
                normalizer.normalize(doc)
            }
        } catch (Exception e) {
            log.warn "Could not normalize document (${doc}: $e, e)"
        }
    }

    String getUserData(String id) {
        return storage.getUserData(id)
    }

    boolean storeUserData(String id, String data) {
        return storage.storeUserData(id, data)
    }

    void removeUserData(String id) {
        storage.removeUserData(id)
    }

    private static boolean batchJobThread() {
        return Thread.currentThread().getThreadGroup().getName().contains("whelktool")
    }

    ZoneId getTimezone() {
        return timezone
    }

    private void createCacheRecordsAndPlaceholders(Document postUpdateDoc, Document preUpdateDoc = null) {
        Set<Link> postUpdateLinks = postUpdateDoc.getExternalRefs()
        Set<Link> preUpdateLinks = preUpdateDoc?.getExternalRefs() ?: new HashSet<Link>() //Collections.EMPTY_SET groovy compiler...?
        
        def iris = { Set<Link> s -> s.collect { it.iri } as Set<String> }
        Set<String> addedIris = iris(postUpdateLinks) - iris(preUpdateLinks)

        createCacheRecordsAndPlaceholders(iris(postUpdateLinks), !postUpdateDoc.isCacheRecord())
    }

    private void createCacheRecordsAndPlaceholders(Set<String> iris, boolean tryFetchExternal) {
        Set<String> brokenOrExternalIris = iris - storage.getSystemIdsByIris(iris).keySet()

        boolean minorUpdate = true
        def changedIn = 'xl'
        def changedBy = 'https://libris.kb.se/library/SEK' // FIXME...
        def collection = LegacyIntegrationTools.NO_MARC_COLLECTION
        def deleted = false
        
        brokenOrExternalIris.each { iri -> 
            def doc = tryFetchExternal 
                    ? external.get(iri).orElse(ExternalEntities.getPlaceholder(iri))
                    : ExternalEntities.getPlaceholder(iri)

            try {
                createDocument(doc, changedIn, changedBy, collection, deleted)
            }
            catch (StorageCreateFailedException ignored) {
                // Another transaction already created it -> OK 
            }
        }
        
        // Check if old placeholder records can be replaced with cache records
        bulkLoad(iris - brokenOrExternalIris).values()
                .findAll{doc -> doc.isPlaceholder() }
                .each { doc ->
                    try {
                        external.getEphemeral(doc.getThingIdentifiers().first()).ifPresent({ extDoc ->
                            def checkSum = doc.getChecksum(jsonld)
                            extDoc.setRecordId(doc.getRecordIdentifiers().first())
                            storeAtomicUpdate(extDoc, minorUpdate, changedIn, changedBy, checkSum)
                        })
                    }
                    catch (Exception e) { // TODO 
                        log.warn("Failed to update ${doc.shortId}: $e", e)
                    }
                }
    }
}

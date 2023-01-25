package whelk

import com.google.common.collect.Iterables
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import se.kb.libris.Normalizers
import whelk.component.CachingPostgreSQLComponent
import whelk.component.DocumentNormalizer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.component.PostgreSQLComponent.UpdateAgent
import whelk.component.SparqlUpdater
import whelk.converter.marc.MarcFrameConverter
import whelk.converter.marc.RomanizationStep
import whelk.exception.StorageCreateFailedException
import whelk.filter.LanguageLinker
import whelk.exception.WhelkException
import whelk.filter.LinkFinder
import whelk.filter.NormalizerChain
import whelk.meta.WhelkConstants
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.PropertyLoader

import java.time.ZoneId

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

    boolean completeCore = false

    String applicationId
    Map<String, Map<String, String>> namedApplications = [:]

    String systemContextUri
    String vocabUri
    String vocabDisplayUri

    ZoneId timezone = ZoneId.of('UTC')
    List<String> locales = ['en']

    Map displayData
    Map vocabData
    Map contextData
    JsonLd jsonld

    MarcFrameConverter marcFrameConverter
    RomanizationStep.LanguageResources languageResources 
    ElasticFind elasticFind
    Relations relations
    DocumentNormalizer normalizer

    URI baseUri = null
    boolean skipIndex = false
    boolean skipIndexDependers = false
    
    // useCache may be set to true only when doing initial imports (temporary processes with the rest of Libris down).
    // Any other use of this results in a "local" cache, which will not be invalidated when data changes elsewhere,
    // resulting in potential serving of stale data.

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
        applicationId = configuration.applicationId
        if (!applicationId) {
            throw new WhelkException("Missing required configuration property: applicationId")
        }
        namedApplications = collectNamedApplications(configuration)

        systemContextUri = configuration.systemContextUri
        if (!systemContextUri) {
            throw new WhelkException("Missing required configuration property: systemContextUri")
        }

        if (configuration.baseUri) {
            baseUri = new URI((String) configuration.baseUri)
        }

        if (configuration.timezone) {
            timezone = ZoneId.of((String) configuration.timezone)
        }

        if (configuration.locales) {
            locales = ((String) configuration.locales).split(',').collect { it.trim() }
        }

        loadCoreData(systemContextUri)

        sparqlUpdater = SparqlUpdater.build(storage, jsonld.context, configuration)
    }

    static Map<String, Map<String, String>> collectNamedApplications(Properties configuration) {
        Map apps = [:]
        for (int i = 0; true; i++) {
            def appId = configuration["namedApplications[${i}].id" as String]
            if (!appId) {
                break
            }

            def app = [id: appId]
            def alias = configuration["namedApplications[${i}].alias" as String]
            if (alias) {
                app['alias'] = alias
            }
            apps[appId] = app
        }
        return apps
    }

    synchronized MarcFrameConverter getMarcFrameConverter() {
        if (!marcFrameConverter) {
            marcFrameConverter = new MarcFrameConverter(new LinkFinder(storage), jsonld, languageResources)
        }

        return marcFrameConverter
    }

    Relations getRelations() {
        return relations
    }

    void loadCoreData(String systemContextUri) {
        contextData = loadData(systemContextUri) ?: [:]
        if (!checkCompleteData(contextData)) {
            return
        }

        Map<String, Map> context = JsonLd.getNormalizedContext(contextData)
        vocabUri = context[JsonLd.VOCAB_KEY]
        vocabData = loadData(vocabUri)
        if (!checkCompleteData(vocabData)) {
            return
        }

        vocabDisplayUri = JsonLd.getDisplayUri(vocabUri, vocabData)
        displayData = loadData(vocabDisplayUri)
        if (!checkCompleteData(displayData)) {
            return
        }

        setJsonld(new JsonLd(contextData, displayData, vocabData, locales))

        completeCore = true
        log.info("Loaded with core data")
    }

    boolean checkCompleteData(Map data) {
        if (data == null || data.size() == 0) {
            log.warn("Whelk is in an incomplete core state")
            setJsonld(new JsonLd(contextData ?: [:], displayData ?: [:], vocabData ?: [:], locales))
            return false
        }
        return true
    }

    Map loadData(String uri) {
        return this.storage.getDocumentByIri(uri)?.data
    }

    void setJsonld(JsonLd jsonld) {
        this.jsonld = jsonld
        storage.setJsonld(jsonld)
        if (elastic) {
            elasticFind = new ElasticFind(new ESQuery(this))
            initDocumentNormalizers(elasticFind)
        }
    }

    // FIXME: de-KBV/Libris-ify: some of these are KBV specific, is that a problem?
    private void initDocumentNormalizers(ElasticFind elasticFind) {
        LanguageLinker languageLinker = new LanguageLinker()
        Normalizers.loadDefinitions(languageLinker, this)
        normalizer = new NormalizerChain(
                [
                        Normalizers.nullRemover(),
                        Normalizers.workPosition(jsonld),
                        Normalizers.typeSingularity(jsonld),
                        Normalizers.language(languageLinker),
                        Normalizers.identifiedBy(),
                ] + Normalizers.heuristicLinkers(this, languageLinker.getTypes())
        )
        
        def idsToThings = { String type -> 
            bulkLoad(elasticFind.findIds([(JsonLd.TYPE_KEY): [type]]).collect())
            .collect { _, doc -> (doc.data[JsonLd.GRAPH_KEY] as List)[1] }
            .collectEntries { [it[JsonLd.ID_KEY], it] }
        }
        languageResources = new RomanizationStep.LanguageResources(
                languageLinker: languageLinker,
                languages: elasticFind.find([(JsonLd.TYPE_KEY): ['Language']]).collect(),
                transformedLanguageForms: idsToThings('TransformedLanguageForm'),
                scripts: idsToThings('Script')
        )
    }

    Document getDocument(String id) {
        Document doc = storage.load(id)
        if (doc && baseUri) {
            doc.baseUri = baseUri
        }
        return doc
    }

    Map<String, Document> bulkLoad(Collection<String> ids) {
        def idMap = [:]
        def otherIris = []
        List<String> systemIds = []
        ids.each { id ->
            if (id.startsWith(Document.BASE_URI.toString())) {
                def systemId = Document.BASE_URI.resolve(id).getPath().substring(1)
                idMap[systemId] = id
                systemIds << systemId
            }
            else if (JsonLd.looksLikeIri(id)) {
                otherIris << id
            }
            else {
                systemIds << id
            }
        }
        if (otherIris) {
            Map<String, String> idToIri = storage.getSystemIdsByIris(otherIris)
                    .collectEntries { k, v -> [(v): k] }
            
            systemIds.addAll(idToIri.keySet())
            idMap.putAll(idToIri)
        }
        
        return storage.bulkLoad(systemIds)
                .findAll { id, doc -> !doc.deleted }
                .collectEntries { id, doc -> [(idMap.getOrDefault(id, id)) : doc]}
    }
    
    private void reindexUpdated(Document updated, Document preUpdateDoc) {
        indexAsyncOrSync {
            elastic.index(updated, this)
            
            if (!skipIndexDependers) {
                if (hasChangedMainEntityId(updated, preUpdateDoc)) {
                    reindexAllLinks(updated.shortId)
                } else {
                    reindexAffected(updated, preUpdateDoc.getExternalRefs(), updated.getExternalRefs())
                }
            }
        }
    }
    
    private void indexAsyncOrSync(Runnable runnable) {
        if (skipIndex) {
            return
        }
        
        if(!elastic) {
            log.warn("Elasticsearch not configured when trying to reindex")
            return
        }
        
        Runnable reindex = {
            try {
                runnable.run()
            }
            catch (Exception e) {
                log.error("Error reindexing: $e", e)
            }
        }
        
        if (isBatchJobThread()) {
            // Update them synchronously
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

    private void reindexAffected(Document document, Set<Link> preUpdateLinks, Set<Link> postUpdateLinks) {
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

        boolean success = storage.createDocument(document, changedIn, changedBy, collection, deleted)
        if (success) {
            indexAsyncOrSync {
                elastic.index(document, this)
                if (!skipIndexDependers) {
                    reindexAffected(document, new TreeSet<>(), document.getExternalRefs())
                }
            }
            sparqlUpdater?.pollNow()
        }
        return success
    }

    /**
     * The UpdateAgent SHOULD be a pure function since the update will be retried in case the document
     * was modified in another transaction.
     *
     * Parameter explanation:
     * minorUpdate - When set to true, the 'modified' timestamp will not be updated. This results in no exports being triggered.
     * writeIdenticalVersions - When set to true, a new entry will be written to the versions table even if the data did not change.
     *
     * Returns true if anything was written.
     */
    boolean storeAtomicUpdate(String id, boolean minorUpdate, boolean writeIdenticalVersions, String changedIn, String changedBy, UpdateAgent updateAgent) {
        Document preUpdateDoc = null
        Document updated = storage.storeUpdate(id, minorUpdate, writeIdenticalVersions, changedIn, changedBy, { Document doc ->
            preUpdateDoc = doc.clone()
            updateAgent.update(doc)
            normalize(doc)
        })

        if (updated == null || preUpdateDoc == null) {
            return false
        }
   
        reindexUpdated(updated, preUpdateDoc)
        sparqlUpdater?.pollNow()

        return true
    }

    void storeAtomicUpdate(Document doc, boolean minorUpdate, boolean writeIdenticalVersions, String changedIn, String changedBy, String oldChecksum) {
        normalize(doc)
        Document preUpdateDoc = storage.load(doc.shortId)
        Document updated = storage.storeAtomicUpdate(doc, minorUpdate, writeIdenticalVersions, changedIn, changedBy, oldChecksum)

        if (updated == null) {
            return
        }
        
        reindexUpdated(updated, preUpdateDoc)
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
  
    void remove(String id, String changedIn, String changedBy, boolean force=false) {
        log.debug "Deleting ${id} from Whelk"
        Document doc = storage.load(id)
        storage.remove(id, changedIn, changedBy, force)
        indexAsyncOrSync {
            elastic.remove(id)
            if (!skipIndexDependers) {
                reindexAffected(doc, doc.getExternalRefs(), Collections.emptySet())
            }
        }
    }

    static boolean hasChangedMainEntityId(Document updated, Document preUpdateDoc) {
        preUpdateDoc.getThingIdentifiers()[0] &&
                updated.getThingIdentifiers()[0] &&
                updated.getThingIdentifiers()[0] != preUpdateDoc.getThingIdentifiers()[0]
    }

    void embellish(Document document, List<String> levels = null) {
        def docsByIris = { List<String> iris -> bulkLoad(iris).values().collect{ it.data } }
        Embellisher e = new Embellisher(jsonld, docsByIris, storage.&getCards, relations.&getByReverse)

        if (levels) {
            e.setEmbellishLevels(levels)
        }
        else if (document.getThingType() == 'Item') {
            e.setEmbellishLevels(['cards'])
            e.setFollowInverse(false)
        }

        e.embellish(document)
    }

    /**
     * Get cards
     * @param iris
     * @return map from all thing and record identifiers (including sameAs) to corresponding card of whole document 
     */
    Map<String, Map> getCards(Iterable<String> iris) {
        Map<String, Map> result = [:]
        storage.getCards(iris).each { card ->
            List<Map> graph = (List<Map>) card[JsonLd.GRAPH_KEY]
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

            // TODO: just ensure that normalizers don't trip on these?
            if (doc.data.containsKey(JsonLd.CONTEXT_KEY)) {
                log.info "Skipping DocumentNormalizer step for $doc.id containing ${JsonLd.CONTEXT_KEY}"
                return
            }
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

    private static boolean isBatchJobThread() {
        return Thread.currentThread().getThreadGroup().getName().contains(WhelkConstants.BATCH_THREAD_GROUP)
    }

    ZoneId getTimezone() {
        return timezone
    }
}

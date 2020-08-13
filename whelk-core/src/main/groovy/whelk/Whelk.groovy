package whelk

import com.google.common.collect.Iterables
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import se.kb.libris.Normalizers
import whelk.component.CachingPostgreSQLComponent
import whelk.component.DocumentNormalizer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.exception.StorageCreateFailedException
import whelk.filter.LinkFinder
import whelk.filter.NormalizerChain
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.LegacyIntegrationTools
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
    Map displayData
    Map vocabData
    Map contextData
    JsonLd jsonld
    MarcFrameConverter marcFrameConverter
    Relations relations
    DocumentNormalizer normalizer
    ElasticFind elasticFind
    ZoneId timezone = ZoneId.of('Europe/Stockholm')

    URI baseUri = null

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
        if (configuration.baseUri) {
            whelk.baseUri = new URI((String) configuration.baseUri)
        }
        if (configuration.timezone) {
            whelk.timezone = ZoneId.of((String) configuration.timezone)
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
        if (configuration.timezone) {
            whelk.timezone = ZoneId.of((String) configuration.timezone)
        }
        whelk.loadCoreData()
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

    Whelk() {
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
                        //FIXME: This is KBV specific stuff
                        Normalizers.workPosition(jsonld),
                        Normalizers.language(this),
                        Normalizers.contributionRole(this)
                ]
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

    Map<String, Document> bulkLoad(List<String> ids) {
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
        if (elastic) {
            String collection = LegacyIntegrationTools.determineLegacyCollection(updated, jsonld)
            elastic.index(updated, collection, this)

            // The updated document has changed mainEntity URI (link target)
            if (preUpdateDoc.getThingIdentifiers()[0] &&
                    updated.getThingIdentifiers()[0] &&
                    updated.getThingIdentifiers()[0] != preUpdateDoc.getThingIdentifiers()[0]) {
                reindexAllLinks(updated.shortId)
            } else
                reindexAffected(updated, preUpdateDoc.getExternalRefs())
        }
    }

    private void reindexAffected(Document document, Set<Link> preUpdateLinks) {
        Runnable reindex = {
            Set<Link> postUpdateLinks = document.getExternalRefs()
            Set<Link> removedLinks = (preUpdateLinks - postUpdateLinks)
            Set<Link> addedLinks = (postUpdateLinks - preUpdateLinks)
            reindexAffected(document, addedLinks, removedLinks)
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
        for (String idToReindex : links) {
            Document docToReindex = storage.load(idToReindex)
            elastic.index(docToReindex, storage.getCollectionBySystemID(idToReindex), this)
        }
    }

    private void reindexAffected(Document document, Set<Link> addedLinks, Set<Link> removedLinks) {
        removedLinks.findResults { storage.getSystemIdByIri(it.iri) }
                .each{id -> elastic.decrementReverseLinks(id, storage.getCollectionBySystemID(id))}

        if (storage.isCardChanged(document.getShortId())) {
            // TODO: when types (auth, bib...) have been removed from elastic, do bulk index in chunks of size N here
            getAffectedIds(document).each { id ->
                Document doc = storage.load(id)
                elastic.index(doc, storage.getCollectionBySystemID(doc.shortId), this)
            }
        }

        addedLinks.each { link ->
            String id = storage.getSystemIdByIri(link.iri)
            if (id) {
                Document doc = storage.load(id)
                def lenses = ['chips', 'cards', 'full']
                def reverseRelations = lenses.collect{ jsonld.getInverseProperties(doc.data, it) }.flatten()
                if (reverseRelations.contains(link.relation)) {
                    // we added a link to a document that includes us in its @reverse relations, reindex it
                    elastic.index(doc, storage.getCollectionBySystemID(doc.shortId), this)
                }
                else {
                    // just update link counter
                    elastic.incrementReverseLinks(id, storage.getCollectionBySystemID(id))
                }
            }
        }
    }

    /**
     * Find all other documents that need to be re-indexed because of a change in document
     * @param document the changed document
     * @return an Iterable of system IDs.
     */
    private Iterable<String> getAffectedIds(Document document) {
        List<Iterable<String>> queries = []
        for (String iri : document.getThingIdentifiers()) {
            for (String field : ["_links", "_outerEmbellishments"]) {
                queries << elasticFind.findIds(['q': ["*"], (field): [iri]])
            }
        }
        return Iterables.concat(queries)
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
    boolean createDocument(Document document, String changedIn, String changedBy, String collection, boolean deleted, boolean index = true) {
        normalize(document)
        
        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
        }

        boolean success = storage.createDocument(document, changedIn, changedBy, collection, deleted)
        if (success) {
            if (elastic && index) {
                elastic.index(document, collection, this)
                reindexAffected(document, new TreeSet<>())
            }
        }
        return success
    }

    /**
     * The UpdateAgent SHOULD be a pure function since the update will be retried in case the document
     * was modified in another transaction.
     */
    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document preUpdateDoc = null
        Document updated = storage.storeUpdate(id, minorUpdate, changedIn, changedBy, { Document doc ->
            preUpdateDoc = doc.clone()
            updateAgent.update(doc)
            normalize(doc)
        })

        if (updated == null || preUpdateDoc == null) {
            return null
        }

        reindex(updated, preUpdateDoc)
    }

    Document storeAtomicUpdate(Document doc, boolean minorUpdate, String changedIn, String changedBy, String oldChecksum, boolean index = true) {
        normalize(doc)
        Document preUpdateDoc = storage.load(doc.shortId)
        Document updated = storage.storeAtomicUpdate(doc, minorUpdate, changedIn, changedBy, oldChecksum)
        if (updated == null) {
            return null
        }
        if (index) {
            reindex(updated, preUpdateDoc)
        }
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
        if (elastic) {
            elastic.remove(id)
            reindexAffected(doc, doc.getExternalRefs())
            log.debug "Object ${id} was removed from Whelk"
        }
        else {
            log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
        }
    }

    void embellish(Document document, List<String> levels = null) {
        def docsByIris = { List<String> iris -> bulkLoad(iris).values().collect{ it.data } }
        Embellisher e = new Embellisher(jsonld, docsByIris, storage.&getCards, relations.&getByReverse)

        if(levels) {
            e.setEmbellishLevels(levels)
        }

        e.embellish(document)
    }

    Document loadEmbellished(String systemId) {
        Document doc = getDocument(systemId)
        embellish(doc)
        return doc
    }

    List<Document> getAttachedHoldings(List<String> thingIdentifiers) {
        return storage.getAttachedHoldings(thingIdentifiers).collect(this.&loadEmbellished)
    }

    void normalize(Document doc) {
        try {
            doc.normalizeUnicode()

            if (normalizer != null) {
                normalizer.normalize(doc)
            }
        } catch (Exception e) {
            log.warn "Could not normalize document (${doc}: $e, e)"
        }
    }

    private static boolean batchJobThread() {
        return Thread.currentThread().getThreadGroup().getName().contains("whelktool")
    }

    ZoneId getTimezone() {
        return timezone
    }
}

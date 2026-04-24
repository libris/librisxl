package whelk.component

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.codec.binary.Base64
import se.kb.libris.utils.isbn.ConvertException
import se.kb.libris.utils.isbn.Isbn
import se.kb.libris.utils.isbn.IsbnException
import se.kb.libris.utils.isbn.IsbnParser
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException
import whelk.exception.UnexpectedHttpStatusException
import whelk.util.DocumentUtil
import whelk.util.FresnelUtil
import whelk.util.FresnelUtil.Lens
import whelk.util.Unicode

import java.util.concurrent.LinkedBlockingQueue

import static whelk.FeatureFlags.Flag.EXPERIMENTAL_CATEGORY_COLLECTION
import static whelk.FeatureFlags.Flag.EXPERIMENTAL_INDEX_HOLDING_ORGS
import static whelk.FeatureFlags.Flag.INDEX_BLANK_WORKS
import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.JSONLD_ALT_ID_KEY
import static whelk.JsonLd.Platform.CATEGORY_BY_COLLECTION
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.REVERSE_KEY
import static whelk.JsonLd.SEARCH_KEY
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.WORK_KEY
import static whelk.JsonLd.asList
import static whelk.component.ElasticSearch.SystemFields.CARD_STR
import static whelk.component.ElasticSearch.SystemFields.CHIP_STR
import static whelk.component.ElasticSearch.SystemFields.ES_ID
import static whelk.component.ElasticSearch.SystemFields.FLATTENED_LANG_MAP_PREFIX
import static whelk.component.ElasticSearch.SystemFields.IDS
import static whelk.component.ElasticSearch.SystemFields.LINKS
import static whelk.component.ElasticSearch.SystemFields.OUTER_EMBELLISHMENTS
import static whelk.component.ElasticSearch.SystemFields.SEARCH_CARD_STR
import static whelk.component.ElasticSearch.SystemFields.SORT_KEY_BY_LANG
import static whelk.component.ElasticSearch.SystemFields.TOP_STR
import static whelk.exception.UnexpectedHttpStatusException.isBadRequest
import static whelk.exception.UnexpectedHttpStatusException.isNotFound
import static whelk.util.FresnelUtil.Options.NO_FALLBACK
import static whelk.util.FresnelUtil.Options.SKIP_ITEMS
import static whelk.util.FresnelUtil.Options.SKIP_MAP_VOCAB_TERMS
import static whelk.util.FresnelUtil.Options.TAKE_ALL_ALTERNATE
import static whelk.util.Jackson.mapper

@Log
class ElasticSearch {

    static class SystemFields {
        /**
        In ES up until 7.8 we could use the _id field for aggregations and sorting, but it was discouraged
        for performance reasons. In 7.9 such use was deprecated, and since 8.x it's no longer supported, so
        we follow the advice and use a separate field.
        (https://www.elastic.co/guide/en/elasticsearch/reference/8.8/mapping-id-field.html). */
        public static final String ES_ID = '_es_id'

        public static final String LINKS = '_links'
        public static final String OUTER_EMBELLISHMENTS = '_outerEmbellishments'
        public static final String SORT_KEY_BY_LANG = '_sortKeyByLang'

        public static final String IDS = '_ids'
        public static final String TOP_STR = '_topStr'
        public static final String CHIP_STR = '_chipStr'
        public static final String CARD_STR = '_cardStr'
        public static final String SEARCH_CARD_STR = '_searchCardStr'

        public static final String FLATTENED_LANG_MAP_PREFIX = '__'
    }

    private static final Set<String> SEARCH_STRINGS = [
            JsonLd.SEARCH_KEY,
            TOP_STR,
            CHIP_STR,
            CARD_STR,
            SEARCH_CARD_STR
    ] as Set

    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    static final String SEARCH_TYPE = "dfs_query_then_fetch"

    public int maxResultWindow = 10000 // Elasticsearch default (fallback value)
    public int maxTermsCount = 65536 // Elasticsearch default (fallback value)
    
    String mainIndex = null
    Map<String, String> baseTypeToSubIndex = new HashMap<>();
    Map<String, String> subIndexToBaseType = new HashMap<>();
    private List<String> elasticHosts
    private String elasticUser
    private String elasticPassword
    private ElasticClient client
    private ElasticClient bulkClient
    private boolean isPitApiAvailable = false
    private static final int ES_LOG_MIN_DURATION = 2000 // Only log queries taking at least this amount of milliseconds
    private static final String SUB_IX_SEPARATOR = '-'

    private final Queue<Runnable> indexingRetryQueue = new LinkedBlockingQueue<>()

    private final JsonLd jsonLd

    private static final class DerivedLenses {
        public static final FresnelUtil.Lens CARD_ONLY = new FresnelUtil.Lens(
                FresnelUtil.CARD_CHAIN,
                FresnelUtil.Lenses.SEARCH_CHIP,
                List.of(FresnelUtil.CHIP_CHAIN)
        )

        public static final FresnelUtil.Lens SEARCH_CARD_ONLY = new FresnelUtil.Lens(
                new FresnelUtil.LensGroupChain(FresnelUtil.SEARCH_CARDS),
                FresnelUtil.Lenses.SEARCH_CHIP,
                List.of(FresnelUtil.CHIP_CHAIN, FresnelUtil.CARD_CHAIN)
        )
    }

    ElasticSearch(Properties props, JsonLd jsonLd) {
        this(
                props.getProperty("elasticHost"),
                props.getProperty("elasticIndex"),
                Optional.ofNullable(props.getProperty("elasticSubIndexTypes"))
                        .map { it.split(",") as List }
                        .map { it.collect(s -> s.trim()) }
                        .orElse(Collections.emptyList()),
                props.getProperty("elasticUser"),
                props.getProperty("elasticPassword"),
                jsonLd
        )
    }

    private ElasticSearch(
            String elasticHost,
            String elasticIndex,
            List<String> elasticSubIndexTypes,
            String elasticUser,
            String elasticPassword,
            JsonLd jsonLd)
    {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.mainIndex = elasticIndex
        this.elasticUser = elasticUser
        this.elasticPassword = elasticPassword
        this.jsonLd = jsonLd

        client = ElasticClient.withDefaultHttpClient(elasticHosts, elasticUser, elasticPassword)
        bulkClient = ElasticClient.withBulkHttpClient(elasticHosts, elasticUser, elasticPassword)

        new Timer("ElasticIndexingRetries", true).schedule(new TimerTask() {
            void run() {
                indexingRetryQueue.size().times {
                    Runnable entry = indexingRetryQueue.poll()
                    if (entry != null)
                        entry.run()
                }
            }
        }, 60*1000, 10*1000)
        
        initSettings()

        // initSettings() waits for ES available. Do this after
        for (var type : elasticSubIndexTypes) {
            // is this a physical index with a numerical suffix? it is the case when indexing to a new index
            var suffix = mainIndex.find('_\\d+$')
            var base = suffix ? Unicode.stripSuffix(mainIndex, suffix) : mainIndex
            // elastic index names must be lowercase
            var ix = base + SUB_IX_SEPARATOR + type.toLowerCase() + (suffix ?: '')

            if (indexExists(ix)) {
                baseTypeToSubIndex.put(type, ix);
                subIndexToBaseType.put(ix, type);
            } else {
                log.info("Could not find subindex ${ix}. Disabled it.")
            }
        }

        log.info("Hosts: ${elasticHosts}")
        log.info("Index: ${mainIndex}")
        log.info("Subindices: ${getSubIndexNames()}")
    }

    void initSettings() {

        /* If ES is down when we're starting up, it causes a chain-reaction where the servlet is restarted
           over and over with a pg connection pool that cannot be GC'ed, which eventually leads to system
           collapse. Better to hang here until ES is available.
         */
        Map indexSettings = null
        while (indexSettings == null) {
            try {
                indexSettings = getSettings()
            } catch (Exception e) {
                log.warn("Could not get settings from ES, retrying in 10 seconds (cannot proceed without them)..", e)
                Thread.sleep(10000)
            }
        }
        
        def getInt = { String name, int defaultTo ->
            indexSettings.index && indexSettings.index[name] && ((String) indexSettings.index[name]).isNumber()
                ? ((String) indexSettings.index[name]).toInteger()
                : defaultTo
        }
        
        maxResultWindow = getInt('max_result_window', maxResultWindow)
        maxTermsCount = getInt('max_terms_count', maxTermsCount)
                
        Map clusterInfo = performRequest('GET', '/')
        if (clusterInfo?.version?.build_flavor == 'default') {
            isPitApiAvailable = true
        }
    }

    private List<String> getElasticHosts(String elasticHost) {
        List<String> hosts = []
        for (String host : elasticHost.split(",")) {
            host = host.trim()
            if (!host.contains(":"))
                host += ":9200"
            hosts.add("https://" + host)
        }
        return hosts
    }

    String getIndexName() { mainIndex }

    Collection<String> getSubIndexNames() {
        subIndexToBaseType.keySet()
    }

    @Memoized
    List<String> allIndexNames() {
        [getIndexName()] + getSubIndexNames()
    }

    String getBaseTypeForSubIndex(String subIndex) {
        return subIndexToBaseType[subIndex]
    }

    List<Map<?, ?>> getAllMappings() {
        allIndexNames().collect{ getMappings(it) }
    }

    /**
     * Get ES mappings for associated index
     *
     */
    Map getMappings(String index) {
        Map response
        try {
            response = client.performRequest('GET', "/${index}/_mappings", '')
        } catch (UnexpectedHttpStatusException e) {
            log.warn("Got unexpected status code ${e.statusCode} when getting ES mappings for ${index}: ${e.message}", e)
            return [:]
        }

        // Since ES aliases return the name of the index rather than the alias,
        // we don't rely on names here.
        List<String> keys = response.keySet() as List

        if (keys.size() == 1 && response[(keys[0])].containsKey('mappings')) {
            return (Map) response[(keys[0])]['mappings']
        } else {
            log.warn("Couldn't get mappings from ES index ${index}, response was ${response}.")
            return [:]
        }
    }

    boolean indexExists(String index) {
        try {
            client.performRequest('GET', "/${index}/_settings", '')
            return true
        } catch (UnexpectedHttpStatusException e) {
            if (e.statusCode == 404) {
                return false
            }
            throw e
        }
    }

    /**
     * Get ES settings for associated index
     * NOTE assumes that all subindices have the same settings
     */
    Map getSettings() {
        return getSettings(mainIndex)
    }

    Map getSettings(String index) {
        Map response
        try {
            response = client.performRequest('GET', "/${index}/_settings", '')
        } catch (UnexpectedHttpStatusException e) {
            // When ES is starting up there is a time when it accepts connections but cannot yet authenticate
            // users because the security index is unavailable. This results in a 401 Unauthorized (with the
            // exact same JSON response body as when the security index *IS* available but the credentials are
            // incorrect). Meaning: when initSettings() is executed while ES is starting up, it can get a 401
            // even with correct credentials.
            if (e.getStatusCode() == 401) {
                log.warn("Got unexpected status code ${e.statusCode} when getting ES settings. Either the ES credentials are wrong, or ES has not finished starting up. ${e.message}", e)
            } else {
                log.warn("Got unexpected status code ${e.statusCode} when getting ES settings: ${e.message}", e)
            }
            throw e
        }

        List<String> keys = response.keySet() as List

        if (keys.size() == 1 && response[(keys[0])].containsKey('settings')) {
            return response[(keys[0])]['settings']
        } else {
            throw new RuntimeException("Couldn't get settings from ES index ${indexName}, response was ${response}.")
        }
    }

    int getFieldCount(String index) {
        Map response
        try {
            response = client.performRequest('GET', "/${index}/_field_caps?fields=*", '')
        } catch (Exception e) {
            log.warn("Error getting fields from ES for ${index}: $e", e)
            return -1
        }
        
        try {
            return response.fields.size()
        } catch (Exception e) {
            log.warn("Error parsing response when getting number of fields from ES for ${index}: $e", e)
            return -1
        }
    }

    String getIndexForDoc(Document doc) {
        return getIndexForType(doc.singleThingTypeOrVirtualThingType())
    }

    @Memoized
    String getIndexForType(String type) {
        for (var e : subIndexToBaseType.entrySet()) {
            var subIndex = e.getKey()
            var baseType = e.getValue()
            if (jsonLd.isSubClassOf(type, baseType)) {
                return subIndex
            }
        }

        return mainIndex
    }

    void bulkIndex(Collection<Document> docs, Whelk whelk) {
        if (docs) {
            String bulkString = docs.findResults{ doc ->
                try {
                    String shapedData = getShapeForIndex(doc, whelk)
                    String action = createActionRow(doc)
                    return "${action}\n${shapedData}\n"
                } catch (Exception e) {
                    if (doc.getShortId() == null) {
                        log.error("Document has null shortId, something is wrong. Some details: " + doc.toVerboseString(), e);
                    } else {
                        log.error("Failed to index ${doc.getShortId()} in elastic: $e", e)
                    }
                    return null
                }
            }.join('')

            if (whelk.features.isEnabled(INDEX_BLANK_WORKS)) {
                String bulkString2 = docs
                        .collect { doc -> doc.getVirtualRecordIds().collect {doc.getVirtualRecord(it) } }
                        .flatten()
                        .findResults{ Document doc ->
                            try {
                                String shapedData = getShapeForIndex(doc, whelk)
                                String action = createActionRow(doc)
                                return "${action}\n${shapedData}\n"
                            } catch (Exception e) {
                                if (doc.getShortId() == null) {
                                    log.error("Document has null shortId, something is wrong. Some details: " + doc.toVerboseString(), e);
                                } else {
                                    log.error("Failed to index ${doc.getShortId()} in elastic: $e", e)
                                }
                                return null
                            }
                        }.join('')

                bulkString += bulkString2
            }

            if (bulkString) {
                Map responseMap = bulkClient.performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE)
                int numFailedDueToDocError = 0
                int numFailedDueToESError = 0
                if (responseMap.errors) {
                    responseMap.items?.each { item ->
                        if (item.index?.error) {
                            log.error("Failed indexing document: ${item.index}")
                            if (item.index.status >= 500) {
                                numFailedDueToESError++
                            } else {
                                numFailedDueToDocError++
                            }
                        }
                    }
                }
                int docsCount = docs.count{it}
                int numFailed = numFailedDueToDocError + numFailedDueToESError
                if (numFailed) {
                    log.warn("Tried bulk indexing ${docsCount} docs: ${docsCount - numFailed} succeeded, ${numFailed} failed " +
                            "(${numFailedDueToDocError} due to document error, ${numFailedDueToESError} due to ES error). Took ${responseMap.took} ms")
                    if (numFailedDueToESError) {
                        throw new UnexpectedHttpStatusException("Failed indexing documents due to ES error", 500)
                    }
                } else {
                    log.info("Bulk indexed ${docsCount} docs in ${responseMap.took} ms")
                }
            } else {
                log.warn("Refused bulk indexing ${docs.count{it}} docs because body was empty")
            }
        }
    }

    void bulkIndexWithRetry(Collection<String> ids, Whelk whelk) {
        Collection<Document> docs = whelk.bulkLoad(ids).values()
        try {
            bulkIndex(docs, whelk)
        } catch (Exception e) {
            if (!isBadRequest(e)) {
                log.info("Failed to index batch ${ids} in elastic, placing in retry queue: $e", e)
                indexingRetryQueue.add({ -> bulkIndexWithRetry(ids, whelk) })
            }
            else {
                log.error("Failed to index ${ids} in elastic: $e", e)
            }
        }
    }

    String createActionRow(Document doc) {
        def action = ["index" : [ "_index" : getIndexForDoc(doc),
                                  "_id" : toElasticId(doc.getShortId()) ]]
        return mapper.writeValueAsString(action)
    }

    void index(Document doc, Whelk whelk) {
        // The justification for this uncomfortable catch-all, is that an index-failure must raise an alert (log entry)
        // _internally_ but be otherwise invisible to clients (If postgres writing was ok, the save is considered ok).
        try {
            Map responseMap = client.performRequest(
                    'PUT',
                    "/${getIndexForDoc(doc)}/_doc/${toElasticId(doc.getShortId())}",
                    getShapeForIndex(doc, whelk))
            if (log.isDebugEnabled()) {
                log.debug("Indexed the document ${doc.getShortId()} as ${getIndexForDoc(doc)}/_doc/${responseMap['_id']} as version ${responseMap['_version']}")
            }
        } catch (Exception e) {
            if (!isBadRequest(e)) {
                log.info("Failed to index ${doc.getShortId()} in elastic, placing in retry queue: $e", e)
                indexingRetryQueue.add({ -> index(doc, whelk) })
            }
            else {
                log.error("Failed to index ${doc.getShortId()} in elastic: $e", e)
            }
        }
    }

    void incrementReverseLinks(Document doc, String relation) {
        updateReverseLinkCounter(doc.shortId, relation, 1, getIndexForDoc(doc))
    }

    void decrementReverseLinks(Document doc, String relation) {
        updateReverseLinkCounter(doc.shortId, relation, -1, getIndexForDoc(doc))
    }

    private void updateReverseLinkCounter(String shortId, String relation, int deltaCount, String index) {
        // An indexed document will always have reverseLinks.totalItems set to an integer,
        // and reverseLinks.totalItemsByRelation set to a map, but reverseLinks.totalItemsByRelation['foo']
        // doesn't necessarily exist at this time; hence the null check before trying to update the link counter.
        // The outer "if (ctx._source.reverseLinks.totalItemsByRelation) {}" can be removed once we've
        // reindexed once; it's just there so as to not break things too much before that.
        String body = """
        {
            "script" : {
                "source": "ctx._source.reverseLinks.totalItems += $deltaCount; if (ctx._source.reverseLinks.totalItemsByRelation != null) { if (ctx._source.reverseLinks.totalItemsByRelation['$relation'] == null) { if ($deltaCount > 0) { ctx._source.reverseLinks.totalItemsByRelation['$relation'] = $deltaCount; } } else { ctx._source.reverseLinks.totalItemsByRelation['$relation'] += $deltaCount; } }",
                "lang": "painless"
            }
        }
        """.stripIndent()

        try {
            client.performRequest(
                    'POST',
                    "/${index}/_update/${toElasticId(shortId)}",
                    body)
        }
        catch (Exception e) {
            if (isBadRequest(e)) {
                log.warn("Failed to update reverse link counter ($deltaCount) for $shortId: $e", e)
            }
            else if (isNotFound(e)) {
                // OK. All dependers must be removed before the dependee in lddb. But the index update can happen
                // in any order, so the dependee might already be gone when trying to decrement the counter.
                log.info("Could not update reverse link counter ($deltaCount) for $shortId: $e, it does not exist", e)
            }
            else {
                log.warn("Failed to update reverse link counter ($deltaCount) for $shortId: $e, placing in retry queue.", e)
                indexingRetryQueue.add({ -> updateReverseLinkCounter(shortId, relation, deltaCount, index) })
            }
        }
    }
    
    void remove(String identifier) {
        if (log.isDebugEnabled()) {
            log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        }
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        try {
            Map responseMap = client.performRequest('POST',
                    "/${allIndexNames().join(',')}/_delete_by_query",
                    mapper.writeValueAsString(dsl))

            if (log.isDebugEnabled()) {
                log.debug("Response: ${responseMap.deleted} of ${responseMap.total} objects deleted")
            }
            if (responseMap.deleted == 0) {
                log.warn("Record with id $identifier was not deleted from the Elasticsearch index.")
            }
        }
        catch(Exception e) {
            if (isBadRequest(e)) {
                log.warn("Failed to delete $identifier from index: $e", e)
            }
            else if (isNotFound(e)) {
                log.warn("Tried to delete $identifier from index, but it was not there: $e", e)
            }
            else {
                log.warn("Failed to delete $identifier from index: $e, placing in retry queue.", e)
                indexingRetryQueue.add({ -> remove(identifier) })
            }
        }
    }

    @CompileStatic
    static String getShapeForIndex(Document document, Whelk whelk) {
        Document copy = document.clone()

        whelk.embellish(copy, ['full'])

        if (log.isDebugEnabled()) {
            log.debug("Framing ${document.getShortId()}")
        }

        Set<String> links = whelk.jsonld.expandLinks(document.getExternalRefs()).collect{ it.iri } as Set<String>

        var embellishedGraph = ((List) copy.data[GRAPH_KEY])
        var originalGraphSize = ((List) document.data[GRAPH_KEY]).size()

        Set<String> categoryLinks = [] as Set
        if (whelk.features.isEnabled(EXPERIMENTAL_CATEGORY_COLLECTION)) {
            categoryLinks.addAll(collectCategoryLinks(embellishedGraph))
            links.addAll(categoryLinks)
        }

        FresnelUtil.LensMappingBatch lensedMainGraph = batchToSearchCard(whelk.fresnelUtil,
                embellishedGraph.take(originalGraphSize) as List<Map<String, Object>>,
                links - categoryLinks) // Skip preserving category links since these will be restored in a separate step

        def restoreCategoryByCollection = { shapedGraph, beforeGraph ->
            // FIXME
            [[1], [1, JsonLd.WORK_KEY]].each { List path -> {
                Map thing = (Map) DocumentUtil.getAtPath(beforeGraph, path, [:])
                if (thing.containsKey(CATEGORY_BY_COLLECTION)) {
                    DocumentUtil.getAtPath(shapedGraph, path, [:])[CATEGORY_BY_COLLECTION] = thing[CATEGORY_BY_COLLECTION]
                }
            }}
        }

        var shapedMainGraph = lensedMainGraph.lensedThings()

        if (whelk.features.isEnabled(EXPERIMENTAL_CATEGORY_COLLECTION)) {
            restoreCategoryByCollection(shapedMainGraph, embellishedGraph)
        }

        var integralIds = collectIntegralIds(shapedMainGraph, whelk.jsonld)

        var shapedEmbellished = embellishedGraph
                .drop(originalGraphSize)
                .collect {
                    getShapeForEmbellishment(whelk.fresnelUtil,
                            (Map) it,
                            integralIds,
                            whelk.features.isEnabled(EXPERIMENTAL_CATEGORY_COLLECTION)
                                    ? restoreCategoryByCollection
                                    : null)
                }


        copy.data[GRAPH_KEY] = shapedMainGraph + shapedEmbellished

        setIdentifiers(copy)
        boolean isVirtualWork = copy.isVirtual()
        if (isVirtualWork) {
            copy.centerOnVirtualMainEntity()
        }
        copy.setThingMeta(document.getCompleteId())
        List<String> thingIds = copy.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return copy.data
        }
        String thingId = thingIds.get(0)

        Map searchCard = JsonLd.frame(thingId, copy.data)

        searchCard[LINKS] = links
        searchCard[OUTER_EMBELLISHMENTS] = copy.getEmbellishments() - links

        Map<String, Long> incomingLinkCountByRelation = whelk.getStorage().getIncomingLinkCountByIdAndRelation(stripHash(copy.getShortId()))
        var totalItems = incomingLinkCountByRelation.values().sum(0)

        // These indirect relations shouldn't count towards the total
        // TODO should they be placed somewhere else than totalItemsByRelation?
        // TODO what should be the key "itemOf.instanceOf"?
        // FIXME don't hardcode this
        var itemPath = ["@reverse", "instanceOf", "*", "@reverse", "itemOf", "*"]
        var itemCount = ((List) DocumentUtil.getAtPath(searchCard, itemPath, []))
                .collect{ it['heldBy']?[JsonLd.ID_KEY] }.grep().unique().size()
        incomingLinkCountByRelation.put('itemOf.instanceOf', (long) itemCount)

        searchCard['reverseLinks'] = [
                (JsonLd.TYPE_KEY) : 'PartialCollectionView',
                'totalItems': totalItems,
                'totalItemsByRelation': incomingLinkCountByRelation
        ]

        try {
            searchCard[SORT_KEY_BY_LANG] = buildSortKeyByLang(searchCard, whelk)
        } catch (Exception e) {
            log.error("Couldn't create sort key for {}: {}", document.shortId, e, e)
        }

        try {
            searchCard[CHIP_STR] = whelk.fresnelUtil.asString(searchCard, FresnelUtil.NestedLenses.CHIP_TO_TOKEN, List.of(TAKE_ALL_ALTERNATE, SKIP_ITEMS))
            searchCard[CARD_STR] = whelk.fresnelUtil.asString(searchCard, DerivedLenses.CARD_ONLY, List.of(TAKE_ALL_ALTERNATE, SKIP_ITEMS, NO_FALLBACK))
            searchCard[SEARCH_CARD_STR] = whelk.fresnelUtil.asString(searchCard, DerivedLenses.SEARCH_CARD_ONLY, List.of(TAKE_ALL_ALTERNATE, SKIP_ITEMS, NO_FALLBACK))
        } catch (Exception e) {
            log.error("Couldn't create search fields for {}: {}", document.shortId, e, e)
        }

        searchCard[IDS] = collectIds(embellishedGraph, integralIds)

        DocumentUtil.traverse(searchCard) { value, path ->
            if (path && SEARCH_STRINGS.contains(path.last())) {
                // TODO: replace with elastic ICU Analysis plugin?
                // https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu.html
                if (value instanceof List) {
                    return new DocumentUtil.Replace(((List<String>) value).collect { String s -> !Unicode.isNormalizedForSearch(s) ? Unicode.normalizeForSearch(s) : s })
                }
                if (value instanceof String && !Unicode.isNormalizedForSearch(value)) {
                    return new DocumentUtil.Replace(Unicode.normalizeForSearch(value))
                }
            }

            if (value instanceof Map) {
                try {
                    if (path.isEmpty()) {
                        addSearchStr(value, whelk, FresnelUtil.Lenses.TOP_SEARCH_TOKEN, TOP_STR)
                    } else {
                        addSearchStr(value, whelk, FresnelUtil.Lenses.SEARCH_TOKEN, JsonLd.SEARCH_KEY)
                    }
                } catch (Exception ignored) {
                    log.warn("Couldn't create search key for node with type {} in document {}", value.get(TYPE_KEY), document.shortId);
                }

                lensedMainGraph.restoreLinks(value, isVirtualWork)

                // { "foo": "FOO", "fooByLang": { "en": "EN", "sv": "SV" } }
                // -->
                // { "foo": "FOO", "fooByLang": { "en": "EN", "sv": "SV" }, "__foo": ["FOO", "EN", "SV"] }
                Map<String, List> flattened = [:]
                value.each { k, v ->
                    if (k in whelk.jsonld.langContainerAlias) {
                        var __k = flattenedLangMapKey((String) k)
                        flattened[__k] = ((List) (flattened[__k] ?: [])) + asList(v)
                    } else if (k in whelk.jsonld.langContainerAliasInverted) {
                        var __k = flattenedLangMapKey((String) whelk.jsonld.langContainerAliasInverted[k])
                        flattened[__k] = ((List) (flattened[__k] ?: [])) + ((Map) v).values().flatten()
                    }
                }
                if (!flattened.isEmpty()) {
                    value.putAll(flattened)
                }


                (asList(value['classification']))
            }

            if (path && path.last() == 'classification') {
                addFlattenedClassificationFields(asList(value))
            }

            if ('Item' != searchCard[TYPE_KEY]
                    && path
                    && "heldBy" == path.last()
                    && !path.contains('hasComponent')
                    && value instanceof Map
                    && value[JsonLd.ID_KEY]) {
                if (whelk.features.isEnabled(EXPERIMENTAL_INDEX_HOLDING_ORGS) && !value['isPartOf']) {
                    var org = whelk.relations.getBy((String) value[JsonLd.ID_KEY], ['isPartOf'])
                    if (!org.isEmpty()) {
                        value['isPartOf'] = [(JsonLd.ID_KEY): org.first()]
                    }
                }
                // Libraries may sometimes be embedded in the item via embellish (when appearing as descriptionCreator/descriptionLastModifier).
                // Retain only @id and isPartOf to maintain consistency with non-embedded libraries and avoid indexing unnecessary data.
                return new DocumentUtil.Replace(value.subMap([ID_KEY, 'isPartOf']))
            }

            return DocumentUtil.NOP
        }

        searchCard[ES_ID] = toElasticId(copy.getShortId())

        if (log.isTraceEnabled()) {
            log.trace("Framed data: ${searchCard}")
        }

        return mapper.writeValueAsString(searchCard)
    }

    private static void addFlattenedClassificationFields(List<Map> classification) {
        classification.each { Map c ->
            String type = c[TYPE_KEY]
            String code = c['code']
            if (!code || !type) return
            String flattenedKey = switch (type) {
                case "Classification" -> {
                    String schemeCode = DocumentUtil.getAtPath(c, ['inScheme', 'code'], "")
                    if (schemeCode.toLowerCase().contains('kssb')) {
                        yield '_sab'
                    }
                }
                case "ClassificationDdc" -> '_ddc'
                case "ClassificationUdc" -> '_udc'
                case "ClassificationLcc" -> '_lcc'
                case "ClassificationNlm" -> '_nlm'
                default -> ""
            }
            if (flattenedKey) {
                c[flattenedKey] = code
            }
        }
    }
    
    @CompileStatic
    static String flattenedLangMapKey(String key) {
        return FLATTENED_LANG_MAP_PREFIX + key
    }

    private static Set<String> collectIds(List embellishedGraph, Collection<String> integralIds) {
        var records = embellishedGraph.take(1) + embellishedGraph.findAll { ((String) DocumentUtil.getAtPath(it, Document.thingIdPath2)) in integralIds }
                .collect { DocumentUtil.getAtPath(it, Document.recordPath) }

        Set ids = [] as Set

        records.each {
            ids.add(lastPathSegment((String) it[ID_KEY]))
            DocumentUtil.getAtPath(it, [JSONLD_ALT_ID_KEY, '*', ID_KEY], []).each { ids.add(lastPathSegment((String) it)) }
            ids.addAll(DocumentUtil.getAtPath(it, ['identifiedBy', '*', 'value'], []))
        }

        return ids
    }

    @CompileStatic
    private static Map<String, String> buildSortKeyByLang(Map<String, Object> thing, Whelk whelk) {
        List<String> locales =  whelk.jsonld.locales

        Map<String, String> searchKeyByLang = whelk.fresnelUtil.asStringByLang(thing, FresnelUtil.NestedLenses.CHIP_TO_TOKEN, whelk.jsonld.locales, [])
                .collectEntries {k, v -> [k, cleanForSort(v)] }
                .findAll {!((String) it.value).isEmpty() }

        if (!searchKeyByLang.isEmpty() && searchKeyByLang.size() < locales.size()) {
            // If we have at least one value but not for all locales,
            // duplicate the first available value as a fallback.
            // Example:
            //   {"sv": "xyz"}  ->  {"sv": "xyz", "en": "xyz"}
            String fallback = searchKeyByLang.values().first()
            locales.each { searchKeyByLang.putIfAbsent(it, fallback) }
        }

        return searchKeyByLang
    }

    private static String cleanForSort(String s) {
        // (Copied from JsonLd.applyLensAsMapByLang)
        // Remove leading non-alphanumeric characters.
        // \p{L} = Lu, Ll, Lt, Lm, Lo; but we don't want Lm as it includes modifier letters like
        // MODIFIER LETTER PRIME (ʹ) that are sometimes erroneously used.
        return s.replaceFirst(/^[^\p{Lu}\p{Ll}\p{Lt}\p{Lo}\p{N}]+/, "")
    }

    @CompileStatic
    private static void addSearchStr(Map<String, Object> thing, Whelk whelk, Lens lens, String key) {
        if (!thing[TYPE_KEY]) {
            return
        }

        var langMap = whelk.fresnelUtil.asStringByScript(thing, lens, List.of(NO_FALLBACK))
                ?: whelk.fresnelUtil.asStringByLang(thing, lens, whelk.jsonld.locales, List.of(NO_FALLBACK))

        var values = langMap.values().toList().unique()

        if (!values.isEmpty()) {
            thing.put(key, values.size() == 1 ? values.first() : values)
        }
    }

    @CompileStatic
    private static Set<String> collectIntegralIds(List<Map> graph, JsonLd jsonLd) {
        return JsonLd.getExternalReferences([(GRAPH_KEY): graph])
                .findAll {
                    // FIXME
                    if (jsonLd.isIntegral(it.relation)) {
                        return true
                    }
                    def path = it.propertyPath()
                    return path.size() == 2 && path[0] == REVERSE_KEY && jsonLd.isIntegral(jsonLd.getInverseProperty(path[1]))
                }
                .collect {it.iri }
                .toSet()
    }

    @CompileStatic
    private static Map getShapeForEmbellishment(FresnelUtil fresnelUtil, Map embellishData, Set<String> integralIds, Closure restoreCategoryByCollection) {
        List graph = ((List) embellishData[GRAPH_KEY])
        if (graph) {
            Map record = (Map) graph[0]
            Map thing = (Map) graph[1]
            thing[RECORD_KEY] = record
            Map<String, Object> shapedThing = integralIds.contains(thing[ID_KEY])
                    ? toSearchCard(fresnelUtil, thing) // Do we really want the full search card here?
                    : toSearchChip(fresnelUtil, thing)
            Map shapedRecord = minimalRecord(record) + ((Map) shapedThing.remove(RECORD_KEY) ?: [:])
            List shapedGraph = [shapedRecord, shapedThing]
            if (integralIds.contains(thing[ID_KEY]) && restoreCategoryByCollection) {
                restoreCategoryByCollection(shapedGraph, graph)
            }
            embellishData[GRAPH_KEY] = shapedGraph

        }
        return embellishData
    }

    @CompileStatic
    private static Map<String, Object> toSearchChip(FresnelUtil fresnelUtil, Map thing) {
        return mapThroughLensForIndex(fresnelUtil, thing, FresnelUtil.Lenses.SEARCH_CHIP)
    }

    @CompileStatic
    private static Map<String, Object> toSearchCard(FresnelUtil fresnelUtil, Map thing) {
        return mapThroughLensForIndex(fresnelUtil, thing, FresnelUtil.Lenses.SEARCH_CARD)
    }

    @CompileStatic
    private static Map<String, Object> mapThroughLensForIndex(FresnelUtil fresnelUtil, Map thing, Lens lens) {
        return fresnelUtil.mapThroughLens(thing, lens, [TAKE_ALL_ALTERNATE, SKIP_MAP_VOCAB_TERMS], [])
    }

    @CompileStatic
    private static FresnelUtil.LensMappingBatch batchToSearchCard(FresnelUtil fresnelUtil, List<Map<String, Object>> thing, Collection<String> preserveLinks) {
        return fresnelUtil.mapBatchThroughLens(thing, FresnelUtil.Lenses.SEARCH_CARD, [TAKE_ALL_ALTERNATE, SKIP_MAP_VOCAB_TERMS], preserveLinks)
    }

    @CompileStatic
    private static Map minimalRecord(Map record) {
        return record.subMap([ID_KEY, TYPE_KEY, THING_KEY])
    }

    private static Set<String> collectCategoryLinks(List graph) {
        return [[1], [1, JsonLd.WORK_KEY]].collect { path ->
                    ["find", "identify", "@none"].collect {collection ->
                        DocumentUtil.getAtPath(DocumentUtil.getAtPath(graph, path, [:]),
                                [CATEGORY_BY_COLLECTION, collection, '*', JsonLd.ID_KEY],
                                [])
                    }
                }
                .flatten()
                .toSet()
    }

    private static setIdentifiers(Document doc) {
        DocumentUtil.findKey(doc.data, ["identifiedBy", "indirectlyIdentifiedBy"]) { value, path ->
            if (value !instanceof Collection) {
                return
            }

            var ids = (Collection<Map>) value
            addIdentifierForms(ids, 'ISBN', this::getOtherIsbns)
            addIdentifierForms(ids, 'ISMN', c -> c.findAll{ it.contains("-") }.collect { it.replace("-", "") })
            addIdentifierForms(ids, 'ISNI', this::getFormattedIsnis)
            addIdentifierForms(ids, 'ORCID', this::getFormattedIsnis) // ORCID is a subset of ISNI, same format

            return DocumentUtil.NOP
        }
    }

    @CompileStatic
    private static String lastPathSegment(String uri) {
        uri.contains('/') ? uri.substring(uri.lastIndexOf('/') + 1) : uri
    }

    @CompileStatic
    private static String stripHash(String s) {
        s.contains('#') ? s.substring(0, s.indexOf('#')) : s
    }

    private static void addIdentifierForms(
            Collection<Map> ids,
            String type,
            Closure<Collection<String>> transform)
    {
        var values = ids
                .findAll { (it instanceof Map && it[JsonLd.TYPE_KEY] == type) }
                .findResults{ it['value'] }
        ids.addAll(transform(values).collect { [(JsonLd.TYPE_KEY): type, value: it] })
    }

    private static Collection<String> getOtherIsbns(Collection<String> isbns) {
        isbns.findResults { getOtherIsbnForm(it) }
                .findAll { !isbns.contains(it) }
    }

    private static String getOtherIsbnForm(String isbnValue) {
        Isbn isbn
        try {
            isbn = IsbnParser.parse(isbnValue)
        } catch (IsbnException e) {
            log.warn "Could not parse ISBN ${isbnValue}: $e"
        }
        if (isbn == null) {
            //Isbnparser.parse() returns null for invalid ISBN forms
            return null
        }
        def otherType = isbn.getType() == Isbn.ISBN10 ? Isbn.ISBN13 : Isbn.ISBN10
        try {
            return isbn.convert(otherType).toString()
        } catch (ConvertException ignored) {
            //Exception thrown when trying to transform non-convertible ISBN13 (starting with 979) to ISBN10
            return null
        }
    }

    /**
     * @return ISNIs with four groups of four digits separated by space
     */
    static Collection<String> getFormattedIsnis(Collection<String> isnis) {
        isnis.findAll{ it.size() == 16 }.collect { Unicode.formatIsni(it) }
    }

    Map multiQuery(List jsonDslList, Collection<String> indexNames = Collections.emptyList()) {
        return performQuery(
                jsonDslList.collect { [[:], it].collect { mapper.writeValueAsString(jsonDsl) + '\n' } }.flatten().join(),
                getMultiSearchQueryUrl(indexNames)
        )
    }

    Map query(Map jsonDsl, Collection<String> indexNames = Collections.emptyList()) {
        return performQuery(mapper.writeValueAsString(jsonDsl), getQueryUrl([], indexNames))
    }

    Map queryIds(Map jsonDsl, Collection<String> indexNames = Collections.emptyList()) {
        return performQuery(
                mapper.writeValueAsString(jsonDsl),
                getQueryUrl(['took','hits.total','hits.hits._id'], indexNames)
        )
    }

    /**
     * Find all other documents that need to be re-indexed because 
     * of changes in linked document(s)
     * @param iris of changed document(s)
     * @return an Iterable of system IDs.
     */
    Iterable<String> getAffectedIds(Collection<String> iris) {
        def t1 = iris.collect {['term': [(LINKS): ['value': it]]]}
        def t2 = iris.collect {['term': [(OUTER_EMBELLISHMENTS): ['value': it]]]}
        Map query = [
                'bool': ['should': t1 + t2 ]
        ]

        Scroll<String> ids = new DefaultScroll(query)
        try {
            ids.hasNext()
        }
        catch (TooManyResultsException e) {
            ids = new SearchAfterScroll(query)
        }
        
        return new Iterable<String>() {
            @Override
            Iterator<String> iterator() {
                return ids
            }
        }
    }

    @Override
    int hashCode() {
        return super.hashCode()
    }

    private Map performQuery(String json, String queryUrl) {
        try {
            def start = System.currentTimeMillis()
            Map responseMap = client.performRequest('POST',
                    queryUrl,
                    json)
            def duration = System.currentTimeMillis() - start

            if (duration >= ES_LOG_MIN_DURATION) {
                log.info("ES query took ${duration} (${responseMap.took} server-side)")
            }

            return responseMap
        }
        catch (Exception e) {
            if (isBadRequest(e)) {
                log.debug("Invalid query: $e")
                throw new InvalidQueryException(e.getMessage(), e)
            }
            else {
                log.warn("Failed to query ES: $e")
                throw e
            }
        }
    }

    private String getQueryUrlWithoutIndex(filterPath = []) {
        getQueryUrl(filterPath, null)
    }

    private getMultiSearchQueryUrl(Collection<String> indexNames) {
        return getQueryUrl([], indexNames, true)
    }
    
    private String getQueryUrl(filterPath = [], Collection<String> indexNames, multiSearch = false) {
        boolean noIndex = indexNames == null
        var ix = noIndex ? '' : "/${indexString(indexNames)}"
        def url = ix + (multiSearch ? '/_msearch' : '/_search') + "?search_type=$SEARCH_TYPE"
        if (filterPath) {
            url += "&filter_path=${filterPath.join(',')}"
        }
        return url.toString()
    }

    private String indexString(Collection<String> indexNames) {
        var ixs = indexNames.isEmpty() ? allIndexNames() : indexNames
        ixs.size() == 1
                ? ixs.first()
                : new HashSet<>(ixs).join(",")
    }

    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id.replace('#', '_') // If XL-minted identifier, use the same charsequence
        }
    }
    
    private Map performRequest(String method, String path, Map body = null) {
        try {
            return client.performRequest(method, path, body ? mapper.writeValueAsString(body) : null)
        }
        catch (UnexpectedHttpStatusException e) {
            tryMapAndThrow(e)
            throw e
        }
    }
    
    private abstract class Scroll<T> implements Iterator<T> {
        final int FETCH_SIZE = 500

        protected final List SORT = [[(ES_ID): 'asc']]
        protected final List FILTER_PATH = ['took', 'hits.hits.sort', 'pit_id', 'hits.total.value']

        Iterator<T> fetchedItems
        boolean isBeforeFirstFetch = true

        Map query
        List<String> filterPath
        Closure<T> hitCollector
        
        Scroll(Map query, List<String> hitsFilter = ['hits.hits._id'], Closure<T> hitCollector = { it['_id']}) {
            this.query = query
            this.filterPath = (FILTER_PATH + hitsFilter)
            this.hitCollector = hitCollector
        }

        abstract boolean isAfterLast()
        abstract void updateState(Map response)
        abstract Map nextRequest()
        
        String queryPath() {
            getQueryUrl(filterPath, Collections.emptyList())
        }
        
        @Override
        boolean hasNext() {
            if (isBeforeFirstFetch) {
                fetch()
                isBeforeFirstFetch = false
            }

            return fetchedItems.hasNext() || !isAfterLast()
        }

        @Override
        T next() {
            if (isBeforeFirstFetch) {
                fetch()
                isBeforeFirstFetch = false
            }

            if (!hasNext()) {
                throw new NoSuchElementException()
            }

            if (!fetchedItems.hasNext()) {
                fetch()
            }

            return fetchedItems.next()
        }

        void fetch() {
            Map response = performRequest('POST', queryPath(), nextRequest())
            updateState(response)
            fetchedItems = response.hits.hits.collect(hitCollector).iterator()
        }
    }
    
    private class DefaultScroll<T> extends Scroll<T> {
        int total = -1
        int page = 0

        DefaultScroll(Map query, List<String> hitsFilter = ['hits.hits._id'], Closure<T> hitCollector = { it['_id']}) {
            super(query, hitsFilter, hitCollector)
        }

        @Override
        boolean isAfterLast() {
            return total >= 0 && page * FETCH_SIZE >= total
        }

        @Override
        void updateState(Map response) {
            total = response.hits.total.value
            if (total > maxResultWindow) {
                throw new TooManyResultsException(total, maxResultWindow)
            }

            page++
        }
        
        @Override
        Map nextRequest() {
            return [
                    'query': query,
                    'size': FETCH_SIZE,
                    'from': page * FETCH_SIZE,
                    'track_total_hits': true,
                    'sort': SORT
            ]
        }
    }

    /**
     * Use "search_after" + Point in time API (if available) to be able to retrieve more than 
     * {@link ElasticSearch#maxResultWindow} results. 
     * 
     * When using Point in time API (not available in ElasticSearch OSS version):
     * Caller needs to consume {@link ElasticSearch.Scroll#FETCH_SIZE} results in less time than 
     * {@link SearchAfterScroll#keepAlive} otherwise the search context times out.
     */
    private class SearchAfterScroll<T> extends Scroll<T> {
        final String keepAlive = "1m"

        String pitId = null
        boolean isAfterLastFetch = false

        def offset = null

        SearchAfterScroll(Map query, List<String> hitsFilter = ['hits.hits._id'], Closure<T> hitCollector = { it['_id']}) {
            super(query, hitsFilter, hitCollector)
        }

        @Override
        boolean isAfterLast() {
            return isAfterLastFetch
        }

        @Override
        void updateState(Map response) {
            pitId = response.pit_id
            List items = (List) response.hits.hits
            isAfterLastFetch = items.size() < FETCH_SIZE

            if (isAfterLastFetch && pitId) {
                deletePointInTime(pitId)
            }
                
            if (items) {
                offset = items.last()['sort']
            }
        }

        @Override
        String queryPath() {
            isPitApiAvailable // point in time is created on index and then index cannot be specified here 
                    ? getQueryUrlWithoutIndex(filterPath)
                    : getQueryUrl(filterPath, Collections.emptyList())
        }

        @Override
        Map nextRequest() {
            if (!pitId && isPitApiAvailable) {
                pitId = createPointInTime(keepAlive)
            }

            Map request = [
                    'query': query,
                    'size': FETCH_SIZE,
                    'track_total_hits': false,
                    'sort': SORT
            ]
            
            if (pitId) {
                request['pit'] = [
                        'id': pitId,
                        'keep_alive': keepAlive
                ]
            }
            if (offset) {
                request['search_after'] = offset
            }
            return request
        }
    }

    // TODO support specifying indices?
    private String createPointInTime(String keepAlive = "1m") {
        try {
            return performRequest('POST', "/${allIndexNames().join(',')}/_pit?keep_alive=$keepAlive").id
        }
        catch (Exception e) {
            log.warn("Failed to create Point In Time: $e")
            throw e
        }
    }

    private void deletePointInTime(String id) {
        try {
            Map response = performRequest('DELETE', "/_pit", ['id': id])
            if (!response.succeeded) {
                throw new RuntimeException("DELETE failed, got response: $response")
            }
        }
        catch (UnexpectedHttpStatusException e) {
            if (e.getStatusCode() != 404) {
                log.warn("Failed to delete Point In Time: $e")
                throw e
            }
        }
        catch (Exception e) {
            log.warn("Failed to delete Point In Time: $e")
            throw e
        }
    }

    static class TooManyResultsException extends RuntimeException {
        TooManyResultsException(int results, int max) {
            super("Query resulted in $results results which is more than the $max that can be iterated")
        }
    }
    
    static class SearchContextExpiredException extends RuntimeException {
        SearchContextExpiredException(String msg) {
            super(msg)
        }
    }

    static void tryMapAndThrow(UnexpectedHttpStatusException e) {
        if (e.statusCode == 404 && e.getMessage().contains("search_context_missing_exception")) {
            throw new SearchContextExpiredException(e.getMessage())
        }
    }
}

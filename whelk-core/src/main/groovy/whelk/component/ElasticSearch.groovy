package whelk.component

import groovy.json.JsonOutput
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
import whelk.util.Unicode

import java.util.concurrent.LinkedBlockingQueue

import static whelk.FeatureFlags.Flag.INDEX_BLANK_WORKS
import static whelk.JsonLd.asList
import static whelk.exception.UnexpectedHttpStatusException.isBadRequest
import static whelk.exception.UnexpectedHttpStatusException.isNotFound
import static whelk.util.Jackson.mapper

@Log
class ElasticSearch {
    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    static final String SEARCH_TYPE = "dfs_query_then_fetch"

    // FIXME: de-KBV/Libris-ify: configurable
    static final List<String> REMOVABLE_BASE_URIS = [
            'http://libris.kb.se/',
            'https://libris.kb.se/',
            'http://id.kb.se/vocab/',
            'https://id.kb.se/vocab/',
            'http://id.kb.se/',
            'https://id.kb.se/',
    ]

    public int maxResultWindow = 10000 // Elasticsearch default (fallback value)
    public int maxTermsCount = 65536 // Elasticsearch default (fallback value)
    
    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticUser
    private String elasticPassword
    private ElasticClient client
    private ElasticClient bulkClient
    private boolean isPitApiAvailable = false
    private static final int ES_LOG_MIN_DURATION = 2000 // Only log queries taking at least this amount of milliseconds

    private final Queue<Runnable> indexingRetryQueue = new LinkedBlockingQueue<>()

    ElasticSearch(Properties props) {
        this(
                props.getProperty("elasticHost"),
                props.getProperty("elasticIndex"),
                props.getProperty("elasticUser"),
                props.getProperty("elasticPassword")
        )
    }

    ElasticSearch(String elasticHost, String elasticIndex, String elasticUser, String elasticPassword) {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.defaultIndex = elasticIndex
        this.elasticUser = elasticUser
        this.elasticPassword = elasticPassword

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
                log.warn("Could not get settings from ES, retying in 10 seconds (cannot proceed without them)..", e)
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

    String getIndexName() { defaultIndex }

	/**
	 * Get ES mappings for associated index
	 *
	 */
	Map getMappings() {
        Map response
        try {
            response = mapper.readValue(client.performRequest('GET', "/${indexName}/_mappings", ''), Map)
        } catch (UnexpectedHttpStatusException e) {
            log.warn("Got unexpected status code ${e.statusCode} when getting ES mappings: ${e.message}", e)
            return [:]
        }

        // Since ES aliases return the name of the index rather than the alias,
        // we don't rely on names here.
        List<String> keys = response.keySet() as List

        if (keys.size() == 1 && response[(keys[0])].containsKey('mappings')) {
            return response[(keys[0])]['mappings']
        } else {
            log.warn("Couldn't get mappings from ES index ${indexName}, response was ${response}.")
            return [:]
        }
    }

    /**
     * Get ES settings for associated index
     */
    Map getSettings() {
        Map response
        try {
            response = mapper.readValue(client.performRequest('GET', "/${indexName}/_settings", ''), Map)
        } catch (UnexpectedHttpStatusException e) {
            log.warn("Got unexpected status code ${e.statusCode} when getting ES settings: ${e.message}", e)
            return [:]
        }

        List<String> keys = response.keySet() as List

        if (keys.size() == 1 && response[(keys[0])].containsKey('settings')) {
            return response[(keys[0])]['settings']
        } else {
            log.warn("Couldn't get settings from ES index ${indexName}, response was ${response}.")
            return [:]
        }
    }
    
    int getFieldCount() {
        Map response
        try {
            response = mapper.readValue(client.performRequest('GET', "/${indexName}/_field_caps?fields=*", ''), Map)
        } catch (Exception e) {
            log.warn("Error getting fields from ES: $e", e)
            return -1
        }
        
        try {
            return response.fields.size()
        } catch (Exception e) {
            log.warn("Error parsing response when getting number of fields from ES: $e", e)
            return -1
        }
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
                String response = bulkClient.performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE)
                Map responseMap = mapper.readValue(response, Map)
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
        def action = ["index" : [ "_index" : indexName,
                                  "_id" : toElasticId(doc.getShortId()) ]]
        return mapper.writeValueAsString(action)
    }

    void index(Document doc, Whelk whelk) {
        // The justification for this uncomfortable catch-all, is that an index-failure must raise an alert (log entry)
        // _internally_ but be otherwise invisible to clients (If postgres writing was ok, the save is considered ok).
        try {
            String response = client.performRequest(
                    'PUT',
                    "/${indexName}/_doc/${toElasticId(doc.getShortId())}",
                    getShapeForIndex(doc, whelk))
            if (log.isDebugEnabled()) {
                Map responseMap = mapper.readValue(response, Map)
                log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/_doc/${responseMap['_id']} as version ${responseMap['_version']}")
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

    void incrementReverseLinks(String shortId, String relation) {
        updateReverseLinkCounter(shortId, relation, 1)
    }

    void decrementReverseLinks(String shortId, String relation) {
        updateReverseLinkCounter(shortId, relation, -1)
    }

    private void updateReverseLinkCounter(String shortId, String relation, int deltaCount) {
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
                    "/${indexName}/_update/${toElasticId(shortId)}",
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
                indexingRetryQueue.add({ -> updateReverseLinkCounter(shortId, relation, deltaCount) })
            }
        }
    }
    
    void remove(String identifier) {
        if (log.isDebugEnabled()) {
            log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        }
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        try {
            def response = client.performRequest('POST',
                    "/${indexName}/_delete_by_query",
                    JsonOutput.toJson(dsl))

            Map responseMap = mapper.readValue(response, Map)
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

    String getShapeForIndex(Document document, Whelk whelk) {
        Document copy = document.clone()
        
        whelk.embellish(copy, ['search-chips'])

        if (log.isDebugEnabled()) {
            log.debug("Framing ${document.getShortId()}")
        }

        Set<String> links = whelk.jsonld.expandLinks(document.getExternalRefs()).collect{ it.iri }
        def graph = ((List) copy.data['@graph'])
        int originalSize = document.data['@graph'].size()
        copy.data['@graph'] =
                graph.take(originalSize) +
                graph.drop(originalSize).collect { getShapeForEmbellishment(whelk, it) }
        setIdentifiers(copy)
        if (copy.isVirtual()) {
            copy.centerOnVirtualMainEntity()
        }
        copy.setThingMeta(document.getCompleteId())
        List<String> thingIds = copy.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return copy.data
        }
        String thingId = thingIds.get(0)
        Map framed = toSearchCard(whelk, JsonLd.frame(thingId, copy.data), links)

        framed['_links'] = links
        framed['_outerEmbellishments'] = copy.getEmbellishments() - links

        Map<String, Long> incomingLinkCountByRelation = whelk.getStorage().getIncomingLinkCountByIdAndRelation(stripHash(copy.getShortId()))
        framed['reverseLinks'] = [
                (JsonLd.TYPE_KEY) : 'PartialCollectionView',
                'totalItems': incomingLinkCountByRelation.values().sum(0),
                'totalItemsByRelation': incomingLinkCountByRelation,

                // FIXME don't hardcode this
                // TODO where should they be placed "_totalItemsByIndirectRelation"?
                // TODO what should be the key "itemOf.instanceOf"?
                '_totalItemsByIndirectRelation': [
                        'itemOf.instanceOf': ((List) DocumentUtil.getAtPath(framed, ["@reverse", "instanceOf", "*", "@reverse", "itemOf", "*"], [])).size()
                ],
        ]

        framed['_sortKeyByLang'] = whelk.jsonld.applyLensAsMapByLang(
                framed,
                whelk.jsonld.locales as Set,
                REMOVABLE_BASE_URIS,
                document.getThingInScheme() ? ['tokens', 'chips'] : ['chips'])

        DocumentUtil.traverse(framed) { value, path ->
            if (path && JsonLd.SEARCH_KEY == path.last() && !Unicode.isNormalizedForSearch(value)) {
                // TODO: replace with elastic ICU Analysis plugin?
                // https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu.html
                return new DocumentUtil.Replace(Unicode.normalizeForSearch(value))
            }

            // { "foo": "FOO", "fooByLang": { "en": "EN", "sv": "SV" } }
            // -->
            // { "foo": "FOO", "fooByLang": { "en": "EN", "sv": "SV" }, "__foo": ["FOO", "EN", "SV"] }
            if (value instanceof Map) {
                var flattened = [:]
                value.each { k, v ->
                    if (k in whelk.jsonld.langContainerAlias) {
                        var __k = flattenedLangMapKey(k)
                        flattened[__k] = (flattened[__k] ?: []) + asList(v)
                    } else if (k in whelk.jsonld.langContainerAliasInverted) {
                        var __k = flattenedLangMapKey(whelk.jsonld.langContainerAliasInverted[k])
                        flattened[__k] = (flattened[__k] ?: []) + ((Map) v).values().flatten()
                    }
                }
                value.putAll(flattened)
            }
        }

        // In ES up until 7.8 we could use the _id field for aggregations and sorting, but it was discouraged
        // for performance reasons. In 7.9 such use was deprecated, and since 8.x it's no longer supported, so
        // we follow the advice and use a separate field.
        // (https://www.elastic.co/guide/en/elasticsearch/reference/8.8/mapping-id-field.html).
        framed["_es_id"] =  toElasticId(copy.getShortId())

        if (log.isTraceEnabled()) {
            log.trace("Framed data: ${framed}")
        }

        return JsonOutput.toJson(framed)
    }
    
    static String flattenedLangMapKey(key) {
        return '__' + key
    }

    private static Map toSearchCard(Whelk whelk, Map thing, Set<String> preserveLinks) {
        boolean chipsify = false
        boolean addSearchKey = true
        boolean reduceKey = false
        boolean searchCard = true
        
        whelk.jsonld.toCard(thing, chipsify, addSearchKey, reduceKey, preserveLinks, searchCard)
    }

    private static Map getShapeForEmbellishment(Whelk whelk, Map thing) {
        Map e = toSearchCard(whelk, thing, Collections.EMPTY_SET)
        recordToChip(whelk, e)
        filterLanguages(whelk, e)
        return e
    }

    private static void recordToChip(Whelk whelk, Map thing) {
        if (thing[JsonLd.GRAPH_KEY]) {
            thing[JsonLd.GRAPH_KEY][0] = whelk.jsonld.toChip(thing[JsonLd.GRAPH_KEY][0], [] as Set, true)
        }
    }

    private static void filterLanguages(Whelk whelk, Map thing) {
        DocumentUtil.traverse(thing, { value, path ->
            if (path && path.last() in whelk.jsonld.langContainerAliasInverted) {
                Map<String, String> langContainer = value
                var keep = langContainer.findAll { langTag, str -> langTag in whelk.jsonld.locales }
                
                var transformed = langContainer.findAll { langTag, str -> langTag.contains('-t-') }
                keep.putAll(transformed)
                transformed.keySet().each { tLangTag ->
                    var original = langContainer.findAll { langTag, str -> tLangTag.contains(langTag) }
                    keep.putAll(original)
                }
                
                return new DocumentUtil.Replace(keep)
            }
        })
    }

    private static setIdentifiers(Document doc) {
        DocumentUtil.findKey(doc.data, ["identifiedBy", "indirectlyIdentifiedBy"]) { value, path ->
            if (value !instanceof Collection) {
                return
            }

            var ids = (Collection<Map>) value
            addIdentifierForms(ids, 'ISBN', this::getOtherIsbns)
            addIdentifierForms(ids, 'ISNI', this::getFormattedIsnis)
            addIdentifierForms(ids, 'ORCID', this::getFormattedIsnis) // ORCID is a subset of ISNI, same format

            return DocumentUtil.NOP
        }
    }

    private static String stripHash(String s) {
        s.contains('#') ?s .substring(0, s.indexOf('#')) : s
    }

    private static void addIdentifierForms(
            Collection<Map> ids,
            String type,
            Closure<Collection<String>> transform)
    {
        var values = ids
                .findAll { (it instanceof Map && it[JsonLd.TYPE_KEY] == type) }
                .findResults{ it['value'] }
        ids.addAll(transform(values).collect{ [(JsonLd.TYPE_KEY): type, value: it] })
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
     * @return ISNIs with with four groups of four digits separated by space
     */
    static Collection<String> getFormattedIsnis(Collection<String> isnis) {
        isnis.findAll{ it.size() == 16 }.collect { Unicode.formatIsni(it) }
    }

    Map query(Map jsonDsl) {
        return performQuery(jsonDsl, getQueryUrl())
    }

    Map queryIds(Map jsonDsl) {
        return performQuery(
                jsonDsl,
                getQueryUrl(['took','hits.total','hits.hits._id'])
        )
    }
    
    /**
     * Find all other documents that need to be re-indexed because 
     * of changes in linked document(s)
     * @param iris of changed document(s)
     * @return an Iterable of system IDs.
     */
    Iterable<String> getAffectedIds(Collection<String> iris) {
        def t1 = iris.collect {['term': ['_links': ['value': it]]]}
        def t2 = iris.collect {['term': ['_outerEmbellishments': ['value': it]]]}
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

    private Map performQuery(Map jsonDsl, String queryUrl) {
        try {
            def start = System.currentTimeMillis()
            String responseBody = client.performRequest('POST',
                    queryUrl,
                    JsonOutput.toJson(jsonDsl))

            def duration = System.currentTimeMillis() - start
            Map responseMap = mapper.readValue(responseBody, Map)

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
    
    private String getQueryUrl(filterPath = [], index = indexName) {
        def url = (index ? "/${index}" : '') + "/_search?search_type=$SEARCH_TYPE"
        if (filterPath) {
            url += "&filter_path=${filterPath.join(',')}"
        }
        return url.toString()
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
            return mapper.readValue(client.performRequest(method, path, body ? JsonOutput.toJson(body) : null), Map)    
        }
        catch (UnexpectedHttpStatusException e) {
            tryMapAndThrow(e)
            throw e
        }
    }
    
    private abstract class Scroll<T> implements Iterator<T> {
        final int FETCH_SIZE = 500

        protected final List SORT = [['_es_id': 'asc']]
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
            getQueryUrl(filterPath)
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
                    : getQueryUrl(filterPath)
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
    
    private String createPointInTime(String keepAlive = "1m") {
        try {
            return performRequest('POST', "/$indexName/_pit?keep_alive=$keepAlive").id
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
            log.warn("Failed to create Point In Time: $e")
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

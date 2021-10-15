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

import static whelk.util.Jackson.mapper

@Log
class ElasticSearch {
    static final String BULK_CONTENT_TYPE = "application/x-ndjson"

    static final Set<String> LANGUAGES_TO_INDEX = ['sv', 'en'] as Set
    static final List<String> REMOVABLE_BASE_URIS = [
            'http://libris.kb.se/',
            'https://libris.kb.se/',
            'http://id.kb.se/vocab/',
            'https://id.kb.se/vocab/',
            'http://id.kb.se/',
            'https://id.kb.se/',
    ]

    public int maxResultWindow = 10000 // Elasticsearch default (fallback value)
    
    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticCluster
    private ElasticClient client
    private ElasticClient bulkClient
    private boolean isPitApiAvailable = false

    private final Queue<Runnable> indexingRetryQueue = new LinkedBlockingQueue<>()

    ElasticSearch(Properties props) {
        this(
                props.getProperty("elasticHost"),
                props.getProperty("elasticCluster"),
                props.getProperty("elasticIndex")
        )
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.elasticCluster = elasticCluster
        this.defaultIndex = elasticIndex

        client = ElasticClient.withDefaultHttpClient(elasticHosts)
        bulkClient = ElasticClient.withBulkHttpClient(elasticHosts)

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
        Map indexSettings = getSettings()
        if (indexSettings.index &&
                indexSettings.index['max_result_window'] &&
                ((String) indexSettings.index['max_result_window']).isNumber()) {
            maxResultWindow = ((String) indexSettings.index['max_result_window']).toInteger()
        }
        
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
            hosts.add("http://" + host)
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

    void bulkIndex(Collection<Document> docs, Whelk whelk) {
        if (docs) {
            String bulkString = docs.findResults{ doc ->
                try {
                    String shapedData = getShapeForIndex(doc, whelk)
                    String action = createActionRow(doc)
                    return "${action}\n${shapedData}\n"
                } catch (Exception e) {
                    log.error("Failed to index ${doc.getShortId()} in elastic: $e", e)
                    return null
                }
            }.join('')

            String response = bulkClient.performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE)
            Map responseMap = mapper.readValue(response, Map)
            log.info("Bulk indexed ${docs.count{it}} docs in ${responseMap.took} ms")
        }
    }

    void bulkIndexWithRetry(Collection<String> ids, Whelk whelk) {
        Collection<Document> docs = whelk.bulkLoad(ids).values()
        try {
            bulkIndex(docs, whelk)
        } catch (Exception e) {
            if (!isBadRequest(e)) {
                log.error("Failed to index batch ${ids} in elastic, placing in retry queue: $e", e)
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
                log.error("Failed to index ${doc.getShortId()} in elastic, placing in retry queue: $e", e)
                indexingRetryQueue.add({ -> index(doc, whelk) })
            }
            else {
                log.error("Failed to index ${doc.getShortId()} in elastic: $e", e)
            }
        }
    }

    void incrementReverseLinks(String shortId) {
        updateReverseLinkCounter(shortId, 1)
    }

    void decrementReverseLinks(String shortId) {
        updateReverseLinkCounter(shortId, -1)
    }

    private void updateReverseLinkCounter(String shortId, int deltaCount) {
        String body = """
        {
            "script" : {
                "source": "ctx._source.reverseLinks.totalItems += $deltaCount",
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
            if (isMissingDocument(e)) {
                log.debug("Failed to update reverse link counter for $shortId, missing")
            }
            else if (!isBadRequest(e) ) {
                log.warn("Failed to update reverse link counter for $shortId: $e, placing in retry queue.", e)
                indexingRetryQueue.add({ -> updateReverseLinkCounter(shortId, deltaCount) })
            }
            else {
                log.warn("Failed to update reverse link counter for $shortId: $e", e)
            }
        }
    }

    static boolean isBadRequest(Exception e) {
        e instanceof UnexpectedHttpStatusException && e.getStatusCode() == 400
    }

    static boolean isMissingDocument(Exception e) {
        e instanceof UnexpectedHttpStatusException && e.getStatusCode() == 404
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
            log.warn("Record with id $identifier was not deleted from the Elasticsearch index: $e")
        }
    }

    String getShapeForIndex(Document document, Whelk whelk) {
        Document copy = document.clone()

        whelk.embellish(copy, ['chips'])

        if (log.isDebugEnabled()) {
            log.debug("Framing ${document.getShortId()}")
        }

        Set<String> links = whelk.jsonld.expandLinks(document.getExternalRefs()).collect{ it.iri }

        def graph = ((List) copy.data['@graph'])
        int originalSize = document.data['@graph'].size()
        copy.data['@graph'] =
                graph.take(originalSize).collect { toSearchCard(whelk, it, links) } +
                graph.drop(originalSize).collect { getShapeForEmbellishment(whelk, it) }

        setComputedProperties(copy, links, whelk)
        copy.setThingMeta(document.getCompleteId())
        List<String> thingIds = document.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return copy.data
        }
        String thingId = thingIds.get(0)
        Map framed = JsonLd.frame(thingId, copy.data)
        framed['_sortKeyByLang'] = whelk.jsonld.applyLensAsMapByLang(
                framed,
                LANGUAGES_TO_INDEX,
                REMOVABLE_BASE_URIS,
                document.getThingInScheme() ? ['tokens', 'chips'] : ['chips'])

        // TODO: replace with elastic ICU Analysis plugin?
        // https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-icu.html
        DocumentUtil.findKey(framed, JsonLd.SEARCH_KEY) { value, path ->
            if (!Unicode.isNormalizedForSearch(value)) {
                return new DocumentUtil.Replace(Unicode.normalizeForSearch(value))
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Framed data: ${framed}")
        }

        return JsonOutput.toJson(framed)
    }

    private static Map toSearchCard(Whelk whelk, Map thing, Set<String> preserveLinks) {
        boolean chipsify = false
        boolean addSearchKey = true
        boolean reduceKey = false
        def preservedPaths = preserveLinks ? JsonLd.findPaths(thing, '@id', preserveLinks) : []
        boolean searchCard = true
        
        whelk.jsonld.toCard(thing, chipsify, addSearchKey, reduceKey, preservedPaths, searchCard)
    }

    private static Map getShapeForEmbellishment(Whelk whelk, Map thing) {
        Map e = toSearchCard(whelk, thing, Collections.EMPTY_SET)
        recordToChip(whelk, e)
        filterLanguages(whelk, e)
        return e
    }

    private static void recordToChip(Whelk whelk, Map thing) {
        if (thing[JsonLd.GRAPH_KEY]) {
            thing[JsonLd.GRAPH_KEY][0] = whelk.jsonld.toChip(thing[JsonLd.GRAPH_KEY][0])
        }
    }

    private static void filterLanguages(Whelk whelk, Map thing) {
        Set languageContainers = whelk.jsonld.langContainerAlias.values() as Set
        DocumentUtil.traverse(thing, { value, path ->
            if (path && path.last() in languageContainers) {
                return new DocumentUtil.Replace(value.findAll {lang, str -> lang in LANGUAGES_TO_INDEX})
            }
        })
    }

    private static void setComputedProperties(Document doc, Set<String> links, Whelk whelk) {
        getOtherIsbns(doc.getIsbnValues())
                .each { doc.addTypedThingIdentifier('ISBN', it) }

        getOtherIsbns(doc.getIsbnHiddenValues())
                .each { doc.addIndirectTypedThingIdentifier('ISBN', it) }

        getFormattedIsnis(doc.getIsniValues())
                .each { doc.addTypedThingIdentifier('ISNI', it) }

        getFormattedIsnis(doc.getOrcidValues()) // ORCID is a subset of ISNI, same format
                .each { doc.addTypedThingIdentifier('ORCID', it) }
        
        doc.data['@graph'][1]['_links'] = links
        doc.data['@graph'][1]['_outerEmbellishments'] = doc.getEmbellishments() - links

        doc.data['@graph'][1]['reverseLinks'] = [
                (JsonLd.TYPE_KEY) : 'PartialCollectionView',
                'totalItems' : whelk.getStorage().getIncomingLinkCount(doc.getShortId())]
    }

    private static Collection<String> getOtherIsbns(List<String> isbns) {
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
    private static Collection<String> getFormattedIsnis(Collection<String> isnis) {
        isnis.findAll{ it.size() == 16 }.collect{isni ->
            isni.split("").collate(4).collect{ it.join() }.join(" ")
        }
    }

    Map query(Map jsonDsl) {
        return performQuery(
                jsonDsl,
                getQueryUrl(),
                { def d = it."_source"; d."_id" = it."_id"; return d }
        )
    }

    Map queryIds(Map jsonDsl) {
        return performQuery(
                jsonDsl,
                getQueryUrl() + '?filter_path=took,hits.total,hits.hits._id',
                { it."_id" }
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

    private Map performQuery(Map jsonDsl, String queryUrl, Closure<Map> hitCollector) {
        try {
            def start = System.currentTimeMillis()
            String responseBody = client.performRequest('POST',
                    queryUrl,
                    JsonOutput.toJson(jsonDsl))

            def duration = System.currentTimeMillis() - start
            Map responseMap = mapper.readValue(responseBody, Map)

            log.info("ES query took ${duration} (${responseMap.took} server-side)")

            def results = [:]

            results.startIndex = jsonDsl.from
            results.totalHits = responseMap.hits.total.value
            results.items = responseMap.hits.hits.collect(hitCollector)
            results.aggregations = responseMap.aggregations
            return results
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

    private String getQueryUrl() {
        return "/${indexName}/_search"
    }

    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id // If XL-minted identifier, use the same charsequence
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

        // TODO: change to _shard_doc when we upgrade to ES 7.12+
        protected final List SORT = [['_id': 'asc']]
        protected final List FILTER_PATH = ['took', 'hits.hits.sort', 'pit_id', 'hits.total.value']

        Iterator<T> fetchedItems
        boolean isBeforeFirstFetch = true

        Map query
        String filterPath
        Closure<T> hitCollector
        
        Scroll(Map query, List<String> hitsFilter = ['hits.hits._id'], Closure<T> hitCollector = { it['_id']}) {
            this.query = query
            this.filterPath = (FILTER_PATH + hitsFilter).join(',')
            this.hitCollector = hitCollector
        }

        abstract boolean isAfterLast()
        abstract void updateState(Map response)
        abstract String queryPath()
        abstract Map nextRequest()

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
        String queryPath() {
            "/${indexName}/_search?filter_path=$filterPath"
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
            "/_search?filter_path=$filterPath"
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

package whelk.component

import groovy.json.JsonOutput
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpEntity
import org.apache.http.HttpRequest
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import org.apache.http.util.EntityUtils
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.utils.isbn.ConvertException
import se.kb.libris.utils.isbn.Isbn
import se.kb.libris.utils.isbn.IsbnParser
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.WhelkRuntimeException

@Log
class ElasticSearch {
    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    static final int MAX_CONNECTIONS_PER_HOST = 12
    static final int CONNECTION_POOL_SIZE = 30
    static final int TIMEOUT_MS = 40 * 1000
    static final int MAX_BACKOFF = 1024

    PoolingClientConnectionManager cm
    HttpClient httpClient

    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticCluster
    Random random = new Random()

    private static final ObjectMapper mapper = new ObjectMapper()

    ElasticSearch(Properties props) {
        this.elasticHosts = getElasticHosts(props.getProperty("elasticHost"))
        this.elasticCluster = props.getProperty("elasticCluster")
        this.defaultIndex = props.getProperty("elasticIndex")
        setup()
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elasticHosts = getElasticHosts(elasticHost)
        this.elasticCluster = elasticCluster
        this.defaultIndex = elasticIndex
        setup()
    }

    private List<String> getElasticHosts(String elasticHost) {
        List<String> hosts = []
        for (String host : elasticHost.split(",")) {
            if (!host.contains(":"))
                host += ":9200"
            hosts.add("http://" + host)
        }
        return hosts
    }

    private void setup() {
        cm = new PoolingClientConnectionManager()
        cm.setMaxTotal(CONNECTION_POOL_SIZE)
        cm.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_HOST)

        httpClient = new DefaultHttpClient(cm)
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, TIMEOUT_MS)
        HttpConnectionParams.setSoTimeout(httpParams, TIMEOUT_MS)
        // FIXME: upgrade httpClient (and use RequestConfig)- https://issues.apache.org/jira/browse/HTTPCLIENT-1418
        // httpParams.setParameter(ClientPNames.CONN_MANAGER_TIMEOUT, new Long(TIMEOUT_MS));

        log.info "ElasticSearch component initialized with ${elasticHosts.count{it}} nodes and $CONNECTION_POOL_SIZE workers."
     }

    String getIndexName() { defaultIndex }

	/**
	 * Get ES mappings for associated index
	 *
	 */
	Map getMappings() {
		Tuple2<Integer, String> res = performRequest('GET', "/${indexName}/_mappings", '')
		int statusCode = res.first
		if (statusCode != 200) {
			log.warn("Got unexpected status code ${statusCode} when getting ES mappings.")
			return null
		}
		String responseBody = res.second
		Map response =  mapper.readValue(responseBody, Map)

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

    Tuple2<Integer, String> performRequest(String method, String path, String body, String contentType0 = null) {
        String host = elasticHosts[random.nextInt(elasticHosts.size())]
        HttpRequest request
        switch (method) {
            case 'GET':
                request = new HttpGet(host + path)
                break
            case 'PUT':
                request = new HttpPut(host + path)
                request.setEntity(httpEntity(body, contentType0))
            case 'POST':
                request = new HttpPost(host + path)
                request.setEntity(httpEntity(body, contentType0))
                break
            default:
                throw new IllegalArgumentException("Bad request method:" + method)
        }

        try {
            performRequest(request)
        }
        catch (Exception e) {
            log.warn("Request to ElasticSearch failed: ${e}", e)
            throw new WhelkRuntimeException(e.getMessage(), e)
        }
        finally {
            request.releaseConnection()
        }
    }

    Tuple2<Integer, String> performRequest(HttpRequestBase request) {
        int backOffTime = 1
        while (true) {
            HttpResponse response = httpClient.execute(request)
            String responseBody = EntityUtils.toString(response.getEntity())
            Tuple2<Integer, String> result = new Tuple2(response.getStatusLine().getStatusCode(), responseBody)

            request.reset()

            if (result.first == 429) {
                if (backOffTime > MAX_BACKOFF) {
                    throw new RuntimeException("Max retries exceeded: HTTP 429 from ElasticSearch")
                }

                log.info("Bulk indexing request to ElasticSearch was throttled (HTTP 429) waiting $backOffTime seconds before retry.")
                Thread.sleep(backOffTime * 1000)

                backOffTime *= 2
            }
            else {
                return result
            }
        }
    }

    private HttpEntity httpEntity(String body, String contentType){
        return new StringEntity(body,
                contentType ? ContentType.create(contentType) : ContentType.APPLICATION_JSON)
    }

    void bulkIndex(List<Document> docs, String collection, Whelk whelk) {
        assert collection
        if (docs) {
            String bulkString = docs.collect{ doc ->
                try {
                    String shapedData = JsonOutput.toJson(
                        getShapeForIndex(doc, whelk, collection))
                    String action = createActionRow(doc, collection)
                    return "${action}\n${shapedData}\n"
                } catch (Exception e) {
                    log.error("Failed to index ${doc.getShortId()} in elastic.", e)
                    throw e
                }
            }.join('')

            String response = performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE).second
            Map responseMap = mapper.readValue(response, Map)
            log.info("Bulk indexed ${docs.count{it}} docs in ${responseMap.took} ms")
        }
    }

    String createActionRow(Document doc, String collection) {
        def action = ["index" : [ "_index" : indexName,
                                  "_type" : collection,
                                  "_id" : toElasticId(doc.getShortId()) ]]
        return mapper.writeValueAsString(action)
    }

    void index(Document doc, String collection, Whelk whelk) {
        // The justification for this uncomfortable catch-all, is that an index-failure must raise an alert (log entry)
        // _internally_ but be otherwise invisible to clients (If postgres writing was ok, the save is considered ok).
        try {
            Map shapedData = getShapeForIndex(doc, whelk, collection)
            def response = performRequest('PUT',
                    "/${indexName}/${collection}" +
                            "/${toElasticId(doc.getShortId())}?pipeline=libris",
                    JsonOutput.toJson(shapedData)).second
            Map responseMap = mapper.readValue(response, Map)
            log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${responseMap['_id']} as version ${responseMap['_version']}")
        } catch (Exception e) {
            log.error("Failed to index ${doc.getShortId()} in elastic.", e)
        }
    }

    void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        def response = performRequest('POST',
                "/${indexName}/_delete_by_query?conflicts=proceed",
                JsonOutput.toJson(dsl)).second
        Map responseMap = mapper.readValue(response, Map)
        log.debug("Response: ${responseMap.deleted} of ${responseMap.total} " +
                  "objects deleted")
    }

    Map getShapeForIndex(Document document, Whelk whelk, String collection) {
        Document copy = document.clone()

        if (!collection.equals("hold")) {
            embellish(whelk, document, copy)
        }

        log.debug("Framing ${document.getShortId()}")
        boolean chipsify = false
        boolean addSearchKey = true
        copy.data['@graph'] = copy.data['@graph'].collect { whelk.jsonld.toCard(it, chipsify, addSearchKey) }

        setComputedProperties(copy)
        copy.setThingMeta(document.getCompleteId())
        List<String> thingIds = document.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return copy.data
        }
        String thingId = thingIds.get(0)
        Map framed = JsonLd.frame(thingId, copy.data)

        log.trace("Framed data: ${framed}")

        return framed
    }

    void embellish(Whelk whelk, Document document, Document copy) {
        List externalRefs = document.getExternalRefs()
        List convertedExternalLinks = JsonLd.expandLinks(externalRefs, whelk.jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()))
        Map referencedData = [:]
        Map externalDocs = whelk.bulkLoad(convertedExternalLinks)
        externalDocs.each { id, doc ->
            if (id && doc && doc.hasProperty('data')) {
                referencedData[id] = doc.data
            }
            else {
                log.warn("Could not get external doc ${id} for ${document.getShortId()}, skipping...")
            }
        }
        boolean filterOutNonChipTerms = true // Consider using false here, since cards-in-cards work now.
        whelk.jsonld.embellish(copy.data, referencedData, filterOutNonChipTerms)
    }

    private static void setComputedProperties(Document doc) {
        List<String> identifiedByIsbns = doc.getIsbnValues()
        identifiedByIsbns
                .findResults { getOtherIsbn(it) }
                .findAll { !identifiedByIsbns.contains(it) }
                .each { doc.addTypedThingIdentifier('ISBN', it.toString()) }

        List<String> indirectlyIdentifiedByIsbns = doc.getIsbnHiddenValues()
        indirectlyIdentifiedByIsbns
                .findResults { getOtherIsbn(it) }
                .findAll { !indirectlyIdentifiedByIsbns.contains(it) }
                .each { doc.addIndirectTypedThingIdentifier('ISBN', it.toString()) }
    }

    private static Isbn getOtherIsbn(String isbnValue) {
        Isbn isbn = IsbnParser.parse(isbnValue)
        if (isbn == null) {
            //Isbnparser.parse() returns null for invalid ISBN forms
            return null
        }
        def isbnOtherType = isbn.getType() == Isbn.ISBN10 ? Isbn.ISBN13 : Isbn.ISBN10
        try {
            return isbn.convert(isbnOtherType)
        } catch (ConvertException ignored) {
            //Exception thrown when trying to transform non-convertible ISBN13 (starting with 979) to ISBN10
            return null
        }
    }

    Map query(Map jsonDsl, String collection) {
        return performQuery(
                jsonDsl,
                getQueryUrl(collection),
                { def d = it."_source"; d."_id" = it."_id"; return d }
        )
    }

    Map queryIds(Map jsonDsl, String collection) {
        return performQuery(
                jsonDsl,
                getQueryUrl(collection) + '?filter_path=took,hits.total,hits.hits._id',
                { it."_id" }
        )
    }

    private Map performQuery(Map jsonDsl, String queryUrl, Closure<Map> hitCollector) {
        def start = System.currentTimeMillis()
        Tuple2<Integer, String> response = performRequest('POST',
                queryUrl,
                JsonOutput.toJson(jsonDsl))
        int statusCode = response.first
        String responseBody = response.second
        if (statusCode != 200) {
            log.warn("Unexpected response from ES: ${statusCode} ${responseBody}")
            return [:]
        }
        def duration = System.currentTimeMillis() - start
        Map responseMap = mapper.readValue(responseBody, Map)

        log.info("ES query took ${duration} (${responseMap.took} server-side)")

        def results = [:]

        results.startIndex = jsonDsl.from
        results.totalHits = responseMap.hits.total
        results.items = responseMap.hits.hits.collect(hitCollector)
        results.aggregations = responseMap.aggregations

        return results
    }

    private String getQueryUrl(String collection) {
        String maybeCollection  = ""
        if (collection) {
            maybeCollection = "${collection}/"
        }

        return "/${indexName}/${maybeCollection}_search"
    }

    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id // If XL-minted identifier, use the same charsequence
        }
    }
}

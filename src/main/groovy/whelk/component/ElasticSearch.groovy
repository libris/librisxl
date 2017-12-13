package whelk.component

import groovy.json.JsonOutput
import groovy.util.logging.Log4j2 as Log

import org.apache.commons.codec.binary.Base64
import org.apache.http.entity.ContentType
import org.codehaus.jackson.map.ObjectMapper
import whelk.util.LegacyIntegrationTools
import whelk.util.LongTermHttpConnection
import whelk.Document
import whelk.JsonLd
import whelk.exception.*
import whelk.filter.JsonLdLinkExpander
import whelk.Whelk

import java.util.concurrent.atomic.AtomicBoolean

@Log
class ElasticSearch {

    private class ConnectionPoolEntry {
        public ConnectionPoolEntry(LongTermHttpConnection connection) {
            this.connection = connection
            this.inUse = new AtomicBoolean(false)
        }
        AtomicBoolean inUse
        LongTermHttpConnection connection
    }

    static final int DEFAULT_PAGE_SIZE = 50
    static final String BULK_CONTENT_TYPE = "application/x-ndjson"
    static final int CONNECTION_POOL_SIZE = 9

    Vector<ConnectionPoolEntry> httpConnectionPool = []
    String defaultIndex = null
    private List<String> elasticHosts
    private String elasticCluster

    JsonLdLinkExpander expander

    private static final ObjectMapper mapper = new ObjectMapper()

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex,
                    JsonLdLinkExpander expander=null) {
        this.elasticHosts = []
        for (String host : elasticHost.split(",")) {
            if (!host.contains(":"))
                host += ":9200"
            this.elasticHosts.add("http://" + host)
        }
        this.elasticCluster = elasticCluster
        this.defaultIndex = elasticIndex
        this.expander = expander
        for (int i = 0; i < CONNECTION_POOL_SIZE; ++i) {
            String host = elasticHosts[ i % elasticHosts.size() ]
            httpConnectionPool.add(new ConnectionPoolEntry(new LongTermHttpConnection(host)))
        }
        log.info "ElasticSearch component initialized with ${elasticHosts.count{it}} nodes and $CONNECTION_POOL_SIZE workers."
     }

    String getIndexName() { defaultIndex }

    String performRequest(String method, String path, String body, String contentType0 = null){

        // Get an available connection from the pool
        ConnectionPoolEntry httpConnectionEntry
        int i = 0
        while(true) {
            i++
            if (i == CONNECTION_POOL_SIZE) {
                i = 0
                Thread.yield()
            }

            if (!httpConnectionPool[i].inUse.get()) {
                httpConnectionEntry = httpConnectionPool[i]
                httpConnectionEntry.inUse.set(true)
                break
            }
        }

        String response = null

        try {
            String contentType
            if (contentType0 == null) {
                contentType = ContentType.APPLICATION_JSON.toString()
            } else {
                contentType = contentType0
            }

            LongTermHttpConnection httpConnection = httpConnectionEntry.connection
            int backOffTime = 0
            while (response == null || httpConnection.getResponseCode() == 429) {
                if (backOffTime != 0) {
                    log.info("Bulk indexing request to ElasticSearch was throttled (http 429) waiting $backOffTime seconds before retry.")
                    Thread.sleep(backOffTime * 1000)
                }

                httpConnection.sendRequest(path, method, contentType, body, null, null)
                response = httpConnection.responseData
                httpConnection.clearBuffers()

                if (backOffTime == 0)
                    backOffTime = 1
                else
                    backOffTime *= 2
            }
        } finally {
            httpConnectionEntry.inUse.set(false)
        }

        return response
    }

    void bulkIndex(List<Document> docs, String collection, Whelk whelk,
	               boolean useDocumentCache = false) {
        assert collection
        if (docs) {
            String bulkString = docs.collect{ doc ->
                String shapedData = JsonOutput.toJson(
                    getShapeForIndex(doc, whelk, collection, useDocumentCache))
                String action = createActionRow(doc,collection)
                "${action}\n${shapedData}\n"
            }.join('')

            String response = performRequest('POST', '/_bulk', bulkString, BULK_CONTENT_TYPE)
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
            //def body = new NStringEntity(JsonOutput.toJson(shapedData), ContentType.APPLICATION_JSON)
            def response = performRequest('PUT',
                    "/${indexName}/${collection}" +
                            "/${toElasticId(doc.getShortId())}?pipeline=libris",
                    JsonOutput.toJson(shapedData))
            //def eString = EntityUtils.toString(response.getEntity())
            Map responseMap = mapper.readValue(response, Map)
            log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${responseMap['_id']} as version ${responseMap['_version']}")
        } catch (Exception e) {
            log.error("Failed to index ${doc.getShortId()} in elastic.", e)
        }
    }

    void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        //def query = new NStringEntity(JsonOutput.toJson(dsl), ContentType.APPLICATION_JSON)
        def response = performRequest('POST',
                "/${indexName}/_delete_by_query?conflicts=proceed",
                JsonOutput.toJson(dsl))
        //def eString = EntityUtils.toString(response.getEntity())
        Map responseMap = mapper.readValue(response, Map)
        log.debug("Response: ${responseMap.deleted} of ${responseMap.total} " +
                  "objects deleted")
    }

    Map getShapeForIndex(Document document, Whelk whelk, String collection,
                         boolean useDocumentCache = false) {

        if (!collection.equals("hold")) {
            List externalRefs = document.getExternalRefs()
            List convertedExternalLinks = JsonLd.expandLinks(externalRefs, whelk.jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()))
            Map referencedData = whelk.bulkLoad(convertedExternalLinks, useDocumentCache)
                    .collectEntries { id, doc -> [id, doc.data] }
            whelk.jsonld.embellish(document.data, referencedData, false)
        }

        log.debug("Framing ${document.getShortId()}")
        Map framed = JsonLd.frame(document.getCompleteId(), JsonLd.THING_KEY, document.data)
        log.trace("Framed data: ${framed}")

        return framed
    }

    Map query(Map jsonDsl, String collection) {
        def start = System.currentTimeMillis()
        def response = performRequest('POST',
                getQueryUrl(collection),
                JsonOutput.toJson(jsonDsl))
        def duration = System.currentTimeMillis() - start
        Map responseMap = mapper.readValue(response, Map)

        log.info("ES query took ${duration} (${responseMap.took} server-side)")

        def results = [:]

        results.startIndex = jsonDsl.from
        results.totalHits = responseMap.hits.total
        results.items = responseMap.hits.hits.collect { it."_source" }
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

    // TODO merge with logic in whelk.rest.api.SearchUtils
    // See Jira ticket LXL-122.
    /**
     * Create a DSL query from queryParameters.
     *
     * We treat multiple values for one key as an OR clause, which are
     * then joined with AND.
     *
     * E.g. k1=v1&k1=v2&k2=v3 -> (k1=v1 OR k1=v2) AND (k2=v3)
     */
    static Map createJsonDsl(Map queryParameters, int limit=DEFAULT_PAGE_SIZE,
                             int offset=0) {
        String queryString = queryParameters.remove('q')?.first()
        def dslQuery = ['from': offset,
                        'size': limit]

        List musts = []
        if (queryString == '*') {
            musts << ['match_all': [:]]
        } else if(queryString) {
            musts << ['simple_query_string' : ['query': queryString,
                                                   'default_operator': 'and']]
        }

        List reservedParameters = ['q', 'p', 'o', 'value', '_limit',
                                   '_offset', '_site_base_uri']

        def groups = queryParameters.groupBy {p -> getPrefixIfExist(p.key)}
        Map nested = groups.findAll{g -> g.value.size() == 2}
        List filteredQueryParams = (groups - nested).collect{it.value}

        nested.each { key, vals ->
            musts << buildESNestedClause(key, vals)
        }

        filteredQueryParams.each { Map m ->
            m.each { k, vals ->
                if (k.startsWith('_') || k in reservedParameters) {
                    return


                }
                // we assume vals is a String[], since that's that we get
                // from HttpServletResponse.getParameterMap()
                musts << buildESShouldClause(k, vals)
            }
        }

        dslQuery['query'] = ['bool': ['must': musts]]
        return dslQuery
    }

    static getPrefixIfExist(String key) {
        if (key.contains('.')) {
            return key.substring(0, key.indexOf('.'))
        } else {
            return key
        }
    }

    /*
     * Create a 'bool' query for matching at least one of the values.
     *
     * E.g. k=v1 OR k=v2 OR ..., with minimum_should_match set to 1.
     *
     */
    private static Map buildESShouldClause(String key, String[] values) {
        Map result = [:]
        List shoulds = []

        values.each { v ->
            shoulds << ['match': [(key): v]]
        }

        result['should'] = shoulds
        result['minimum_should_match'] = 1
        return ['bool': result]
    }


    private static Map buildESNestedClause(String prefix, Map nestedQuery) {
        Map result = [:]

        def musts = ['must': nestedQuery.collect {q -> ['match': [(q.key):q.value.first()]] } ]

        result << [['nested':['path': prefix,
                                     'query':['bool':musts]]]]

        return result
    }


    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id // If XL-minted identifier, use the same charsequence
        }
    }
}

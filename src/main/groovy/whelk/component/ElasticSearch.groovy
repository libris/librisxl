package whelk.component

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j as Log

import org.apache.commons.codec.binary.Base64
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient

import whelk.Document
import whelk.JsonLd
import whelk.exception.*
import whelk.filter.JsonLdLinkExpander
import whelk.Whelk

@Log
class ElasticSearch {

    static final int DEFAULT_PAGE_SIZE = 50

    static final String BULK_CONTENT_TYPE = "application/x-ndjson"

    RestClient restClient
    private String elastichost, elasticcluster
    String defaultIndex = null

    boolean haltOnFailure = false

    JsonLdLinkExpander expander

    private static final ObjectMapper mapper = new ObjectMapper()


    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex, JsonLdLinkExpander ex) {
        this.elastichost = elasticHost
        this.elasticcluster = elasticCluster
        this.defaultIndex = elasticIndex
        this.expander = ex
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elastichost = elasticHost
        this.elasticcluster = elasticCluster
        this.defaultIndex = elasticIndex
    }

    private void connectRestClient(){
        if (elastichost) {
            restClient = RestClient.builder(new HttpHost(elastichost,elasticPort,"http")).build()
        }
    }

    Response performRequest(String method, String path, NStringEntity body, String contentType0 = null){
        try {
            connectRestClient()
            String contentType
            if (contentType0 == null) {
                contentType = ContentType.APPLICATION_JSON.toString()
            } else {
                contentType = contentType0
            }

            return restClient.performRequest(method, path,
                    Collections.<String, String> emptyMap(),
                    body,
                    new BasicHeader('content-type', contentType))
        }
        finally {
            restClient.close()
        }
    }

    void bulkIndex(List<Document> docs, String collection, Whelk whelk) {
        assert collection
        if (docs) {
            String bulkString = docs.collect{ doc ->
                String shapedData = JsonOutput.toJson(getShapeForIndex(doc, whelk))
                String action = createActionRow(doc,collection)
                "${action}\n${shapedData}\n"
            }.join('')

            def body = new NStringEntity(bulkString)
            def response = performRequest('POST', '/_bulk',body, BULK_CONTENT_TYPE)
            def eString = EntityUtils.toString(response.getEntity())
            Map responseMap = mapper.readValue(eString, Map)
            log.debug("Bulk indexed ${docs.count{it}} docs in ${responseMap.took}")
        }
    }

    String createActionRow(Document doc, String collection) {
        def action = ["index" : [ "_index" : indexName,
                                  "_type" : collection,
                                  "_id" : toElasticId(doc.getShortId()) ]]
        return mapper.writeValueAsString(action)
    }

    void index(Document doc, String collection, Whelk whelk) {
        Map shapedData = getShapeForIndex(doc, whelk)
        def body = new NStringEntity(JsonOutput.toJson(shapedData), ContentType.APPLICATION_JSON)
        def response = performRequest('PUT',
                "/${indexName}/${collection}" +
                "/${toElasticId(doc.getShortId())}?refresh=wait_for",
                body)
        def eString = EntityUtils.toString(response.getEntity())
        Map responseMap = mapper.readValue(eString, Map)
        log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${responseMap['_id']} as version ${responseMap['_version']}")
    }

    void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        def dsl = ["query":["term":["_id":toElasticId(identifier)]]]
        def query = new NStringEntity(JsonOutput.toJson(dsl), ContentType.APPLICATION_JSON)
        def response = performRequest('POST',
                "/${indexName}/_delete_by_query" +
                "?conflicts=proceed&refresh=wait_for",
                query)
        def eString = EntityUtils.toString(response.getEntity())
        Map responseMap = mapper.readValue(eString, Map)
        log.debug "Response: ${responseMap.deleted} objects deleted"
    }

    Map getShapeForIndex(Document document, Whelk whelk) {

        List externalRefs = document.getExternalRefs()
        List convertedExternalLinks = JsonLd.expandLinks(externalRefs, whelk.jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()))
        Map referencedData = whelk.bulkLoad(convertedExternalLinks).collectEntries { id, doc -> [id, doc.data] }
        whelk.jsonld.embellish(document.data, referencedData)

        log.debug("Framing ${document.getShortId()}")
        Map framed = JsonLd.frame(document.getCompleteId(), JsonLd.THING_KEY, document.data)
        log.trace("Framed data: ${framed}")

        return framed
    }

    Map query(Map jsonDsl, String collection) {
        def query = new NStringEntity(JsonOutput.toJson(jsonDsl), ContentType.APPLICATION_JSON)
        def response = performRequest('POST',
                getQueryUrl(collection),
                query)
        def eString = EntityUtils.toString(response.getEntity())
        Map responseMap = mapper.readValue(eString, Map)

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


    String getIndexName() { defaultIndex }
    int getElasticPort() {
        try { new Integer(elastichost.split(",").first().split(":").last()).intValue() } catch (NumberFormatException nfe) { 9200 }
    }

    static String toElasticId(String id) {
        if (id.contains("/")) {
            return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
        } else {
            return id // If XL-minted identifier, use the same charsequence
        }
    }

    @Deprecated
    String fromElasticId(String id) {
        if (id.contains("::")) {
            log.warn("Using old style index id's for $id")
            def pathelements = []
            id.split("::").each {
                pathelements << URLEncoder.encode(it, "UTF-8")
            }
            return  new String("/"+pathelements.join("/"))
        } else {
            String decodedIdentifier = new String(Base64.decodeBase64(id), "UTF-8")
            log.debug("Decoded id $id into $decodedIdentifier")
            return decodedIdentifier
        }
    }
}

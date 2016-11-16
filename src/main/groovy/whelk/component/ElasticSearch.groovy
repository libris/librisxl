package whelk.component

import groovy.util.logging.Slf4j as Log

import org.apache.commons.codec.binary.Base64
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin
import whelk.Document
import whelk.JsonLd
import whelk.exception.*
import whelk.filter.JsonLdLinkExpander

@Log
class ElasticSearch implements Index {

    static final int WARN_AFTER_TRIES = 1000
    static final int RETRY_TIMEOUT = 300
    static final int MAX_RETRY_TIMEOUT = 60*60*1000
    static final int DEFAULT_PAGE_SIZE = 50

    Client client
    private String elastichost, elasticcluster
    String defaultType = "record"
    String defaultIndex = null

    boolean haltOnFailure = true

    JsonLdLinkExpander expander

    private static final ObjectMapper mapper = new ObjectMapper()


    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex, JsonLdLinkExpander ex) {
        this.elastichost = elasticHost
        this.elasticcluster = elasticCluster
        this.defaultIndex = elasticIndex
        this.expander = ex
        connectClient()
    }

    ElasticSearch(String elasticHost, String elasticCluster, String elasticIndex) {
        this.elastichost = elasticHost
        this.elasticcluster = elasticCluster
        this.defaultIndex = elasticIndex
        connectClient()
    }

    void connectClient() {
        if (elastichost) {
            log.info("Connecting to $elasticcluster using hosts $elastichost")
            def sb = Settings.settingsBuilder()

            if (elasticcluster) {
                sb = sb.put("cluster.name", elasticcluster)
            }
            Settings elasticSettings = sb.build()

            client = TransportClient.builder().settings(elasticSettings).addPlugin(DeleteByQueryPlugin.class).build()
            try {
                elastichost.split(",").each {
                    def host, port
                    if (it.contains(":")) {
                        (host, port) = it.split(":")
                    } else {
                        host = it
                        port = 9300
                    }
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port as int))
                }
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                throw new WhelkRuntimeException("Unable to initialize elasticsearch client. Host configuration might be missing port?")
            }
            log.debug("... connected.")
        } else {
            throw new WhelkRuntimeException("Unable to initialize ES client.")
        }
    }

    public ActionResponse performExecute(ActionRequest request) {
        int failcount = 0
        ActionResponse response = null
        while (response == null) {
            try {
                if (request instanceof IndexRequest) {
                    response = client.index(request).actionGet()
                }
                if (request instanceof BulkRequest) {
                    response = client.bulk(request).actionGet()
                }
                if (request instanceof DeleteRequest) {
                    response = client.delete(request).actionGet()
                }
            } catch (NoNodeAvailableException n) {
                log.trace("Retrying server connection ...")
                if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to connect to elasticsearch after $failcount attempts.")
                }
                if (failcount % 100 == 0) {
                    log.info("Server is not responsive. Still trying ...")
                }
                Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
            }
        }
        return response
    }

    @Override
    public void bulkIndex(List<Document> docs, String collection) {
        assert collection
        if (docs) {
            BulkRequest bulk = new BulkRequest()
            for (doc in docs) {
                try {
                    Map shapedData = getShapeForIndex(doc)
                    bulk.add(new IndexRequest(getIndexName(), collection, toElasticId(doc.getShortId())).source(shapedData))
                } catch (Throwable e) {
                    log.error("Failed to create indexrequest for document ${doc.getShortId()}. Reason: ${e.message}")
                    if (haltOnFailure) {
                        throw e
                    }
                }
            }
            BulkResponse response = performExecute(bulk)
            if (response.hasFailures()) {
                response.iterator().each {
                    if (it.failed) {
                        log.error("Indexing of ${it.id} failed: ${it.failureMessage}")
                    }
                }
            }
        }
    }

    @Override
    public void index(Document doc, String collection) {
        Map shapedData = getShapeForIndex(doc)
        def idxReq = new IndexRequest(getIndexName(), collection, toElasticId(doc.getShortId())).source(shapedData)
        def response = performExecute(idxReq)
        log.debug("Indexed the document ${doc.getShortId()} as ${indexName}/${collection}/${response.getId()} as version ${response.getVersion()}")
    }

    @Override
    public void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        DeleteByQueryResponse rsp = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .setIndices(defaultIndex)
                .setSource(["query":["term":["_id":toElasticId(identifier)]]])
                .execute()
                .actionGet()
        log.debug("Response: ${rsp.totalDeleted}")
    }

    Map getShapeForIndex(Document doc) {
        log.debug("Framing ${doc.getShortId()}")
        Map framed = JsonLd.frame(doc.getCompleteId(), JsonLd.THING_KEY, doc.data)
        log.trace("Framed data: ${framed}")
        /*if (expander) {
            doc = expander.filter(doc)
        }*/
        return framed
    }

    @Override
    Map query(Map jsonDsl, String collection) {
        def idxlist = [defaultIndex] as String[]

        byte[] dsl = mapper.writeValueAsBytes(jsonDsl)
        SearchRequest sr = new SearchRequest(idxlist, dsl)
        sr.searchType(SearchType.DFS_QUERY_THEN_FETCH)
        if (collection) {
          sr.types([collection] as String[])
        }
        def response = client.search(sr).actionGet()

        def results = [:]

        results.startIndex = jsonDsl.from
        results.searchCompletedInISO8601duration = "PT" + response.took.secondsFrac + "S"
        results.totalHits = response.hits.totalHits
        results.items = response.hits.hits.collect { it.source }
        results.aggregations = response.aggregations

        return results
    }


    static Map createJsonDsl(Map queryParameters) {
        // Extract LDAPI parameters
        String pageSize = queryParameters.remove("_pageSize")?.first() ?: ""+DEFAULT_PAGE_SIZE
        String page = queryParameters.remove("_page")?.first() ?: "1"
        String sort = queryParameters.remove("_sort")?.first()
        queryParameters.remove("_where") // Not supported
        queryParameters.remove("_orderBy") // Not supported
        queryParameters.remove("_select") // Not supported
        String queryString = queryParameters.remove("q")?.first()
        def dslQuery = ["from": (Integer.parseInt(page)-1) * (pageSize as int),
                        "size": (pageSize as int)]

        List musts = []
        if (queryString) {
            musts << ['query_string' : ['query': queryString,
                                        'default_operator': 'and']]
        }

        String[] okParams = getWhitelistedQueryParams()
        queryParameters.each { k, vals ->
            if (k in okParams) {
                // we assume vals is a String[], since that's that we get
                // from HttpServletResponse.getParameterMap()
                vals.each { v ->
                    musts << ['match': ["${k}": v]]
                }
            }
        }

        dslQuery['query'] = ['bool': ['must': musts]]
        return dslQuery
    }

    public String getIndexName() { defaultIndex }
    public String getElasticHost() { elastichost.split(":").first() }
    public String getElasticCluster() { elasticcluster }
    public int getElasticPort() {
        try { new Integer(elastichost.split(",").first().split(":").last()).intValue() } catch (NumberFormatException nfe) { 9300 }
    }

    static String[] getWhitelistedQueryParams() {
        // TODO implement this in a better way, preferrably without
        // hardcoding anything
        return ["@type"]
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
                pathelements << java.net.URLEncoder.encode(it, "UTF-8")
            }
            return  new String("/"+pathelements.join("/"))
        } else {
            String decodedIdentifier = new String(Base64.decodeBase64(id), "UTF-8")
            log.debug("Decoded id $id into $decodedIdentifier")
            return decodedIdentifier
        }
    }
}

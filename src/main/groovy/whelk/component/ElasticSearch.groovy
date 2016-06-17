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

    boolean haltOnFailure = false

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
            Settings elasticSettings = sb.build();

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
    public void bulkIndex(List<Document> docs) {
        if (docs) {
            BulkRequest bulk = new BulkRequest()
            for (doc in docs) {
                if (doc.isJson()) {
                    try {
                        doc = getShapeForIndex(doc)
                        bulk.add(new IndexRequest(getIndexName(), (doc.collection ?: defaultType), toElasticId(doc.id)).source(doc.data))
                    } catch (Throwable e) {
                        log.error("Failed to create indexrequest for document ${doc.id}. Reason: ${e.message}")
                        if (haltOnFailure) {
                            throw e
                        }
                    }
                } else {
                    log.warn("Document ${doc.id} is not JSON (${doc.contentType}). Will not index.")
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
    public void index(Document _doc) {
        // Do not modify the passed in document!
        Document doc = new Document(_doc.getId(), Document.deepCopy(_doc.getData()), Document.deepCopy(_doc.getManifest()))
        if (doc.isJson()) {
            doc = getShapeForIndex(doc)
            def idxReq = new IndexRequest(getIndexName(), (doc.collection ?: defaultType), toElasticId(doc.id)).source(doc.data)
            def response = performExecute(idxReq)
            log.debug("Indexed the document ${doc.id} as ${indexName}/${(doc.collection ?: defaultType)}/${response.getId()} as version ${response.getVersion()}")
        } else {
            log.warn("Document ${doc.id} is ${doc.contentType}. Will not index.")
        }
    }

    @Override
    public void remove(String identifier) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")
        DeleteByQueryResponse rsp = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .setIndices(defaultIndex)
                .setSource(["query":["term":["_id":toElasticId(identifier)]]])
                .execute()
                .actionGet();
        log.debug("Response: ${rsp.totalDeleted}")
    }

    Document getShapeForIndex(Document doc) {
        if (doc.isJsonLd()) {
            log.debug("Framing ${doc.id}")
            doc.data = JsonLd.frame(doc.id, JsonLd.THING_KEY, doc.data)
            log.trace("Framed data: ${doc.data}")
            if (expander) {
                doc = expander.filter(doc)
            }
        }
        return doc
    }

    @Override
    Map query(Map jsonDsl, String collection) {
        def idxlist = [defaultIndex] as String[]

        def response = client.search(new SearchRequest(idxlist, mapper.writeValueAsBytes(jsonDsl)).searchType(SearchType.DFS_QUERY_THEN_FETCH).types([collection] as String[])).actionGet()

        def results = [:]
        results.numberOfHits = 0
        results.resultSize = 0
        results.startIndex = jsonDsl.from
        results.searchCompletedInISO8601duration = "PT" + response.took.secondsFrac + "S"
        results.items = []

        if (response) {
            results.resultSize = response.hits.hits.size()
            results.numberOfHits = response.hits.totalHits
            results.items = response.hits.hits.collect { it.source }
        }
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
        def dslQuery = ["from": (Integer.parseInt(page)-1) * (pageSize as int), "size": (pageSize as int)]

        if (queryString) {
            if (queryString == "*") {
                dslQuery["query"] = ["match_all": [:]]
            } else {
                dslQuery["query"] = ['query_string' : ['query': queryString, "default_operator": "and"]]
            }
        }
        for (values in queryParameters) {

        }
        return dslQuery
    }

    public String getIndexName() { defaultIndex }
    public String getElasticHost() { elastichost.split(":").first() }
    public String getElasticCluster() { elasticcluster }
    public int getElasticPort() {
        try { new Integer(elastichost.split(",").first().split(":").last()).intValue() } catch (NumberFormatException nfe) { 9300 }
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

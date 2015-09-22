package whelk.component

import groovy.util.logging.Slf4j as Log

import org.apache.commons.codec.binary.Base64
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.*
import org.elasticsearch.action.delete.*
import whelk.Document
import whelk.JsonLd
import whelk.exception.*
import whelk.filter.JsonLdLinkExpander

@Log
class ElasticSearch implements Index {

    static final int WARN_AFTER_TRIES = 1000
    static final int RETRY_TIMEOUT = 300
    static final int MAX_RETRY_TIMEOUT = 60*60*1000

    Client client
    private String elastichost, elasticcluster
    String defaultType = "record"
    String defaultIndex = null

    JsonLdLinkExpander expander


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
            def sb = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30000)
                .put("client.transport.sniff", true)
            if (elasticcluster) {
                sb = sb.put("cluster.name", elasticcluster)
            }
            Settings elasticSettings = sb.build();
            client = new TransportClient(elasticSettings)
            try {
                elastichost.split(",").each {
                        def (host, port) = it.split(":")
                        if (!port) {
                            port = 9300
                        }
                        client = ((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(host, port as int))
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
        BulkRequest bulk = new BulkRequest()
        for (doc in docs) {
            log.trace("Framing ${doc.id}")
            doc.data = JsonLd.frame(doc.id, doc.data)
            if (expander) {
                doc = expander.filter(doc)
            }

            bulk.add(new IndexRequest(getIndexName(), doc.dataset, toElasticId(doc.id)))
        }
        BulkResponse response = performExecute(bulk)
        if (response.hasFailures()) {
            response.iterator().each {
                if (it.failed) {
                    log.error("Indexing of ${it.id} (${fromElasticId(it.id)}) failed: ${it.failureMessage}")
                }
            }
        }
    }

    @Override
    public void index(Document doc) {
        log.trace("Framing ${doc.id}")
        doc.data = JsonLd.frame(doc.id, doc.data)
        if (expander) {
            doc = expander.filter(doc)
        }
        def idxReq = new IndexRequest(getIndexName(), doc.dataset, toElasticId(doc.id)).source(doc.data)
        def response = performExecute(idxReq)
        log.debug("Indexed the document ${doc.id} as ${indexName}/${doc.dataset}/${response.getId()} as version ${response.getVersion()}")
    }

    @Override
    public void remove(String identifier, String dataset) {
        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")

        client.delete(new DeleteRequest(defaultIndex, dataset, toElasticId(identifier)))
    }



    /*
    void flush() {
        log.debug("Flusing ${this.id}")
        def flushresponse = performExecute(new FlushRequestBuilder(client.admin().indices()))
    }


    public boolean index(String identifier, String dataset, Map data) {
        log.trace("Indexing with identifier $identifier, dataset(type): $dataset, data: $data")
        try {
            IndexResponse response = performExecute(new IndexRequest(getIndexName(), dataset, toElasticId(identifier)).source(data))
            def esIdentifier = response.getId()
            if (esIdentifier) {
                log.debug("Document $identifier indexed with es id: $esIdentifier")
                return true
            }
            throw WhelkIndexException("No elasticsearch identifier received for $identifier")
        } catch (Exception e) {
            log.error("Indexing failed", e)
        }
        return false
    }

    public IndexRequest

    //@Override
    public void remove(String identifier, String dataset) {
        log.debug("Peforming deletebyquery to remove documents extracted from $identifier")
        def delQuery = termQuery("extractedFrom.@id", identifier)
        log.debug("DelQuery: $delQuery")

        def response = performExecute(client.prepareDeleteByQuery(defaultIndex).setQuery(delQuery))

        log.debug("Delbyquery response: $response")
        for (r in response.iterator()) {
            log.debug("r: $r success: ${r.successfulShards} failed: ${r.failedShards}")
        }

        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")

        client.delete(new DeleteRequest(defaultIndex, calculateTypeFromIdentifier(identifier), toElasticId(identifier)))
    }

    SearchResult query(Query q) {
        def indexTypes = []
        if (q instanceof ElasticQuery) {
            for (t in q.indexTypes) {
                if (configuredTypes[t]) {
                    log.debug("Adding configuredTypes for ${t}: ${configuredTypes[t]}")
                    indexTypes.add(t)
                    indexTypes.addAll(configuredTypes[t])
                } else {
                    indexTypes.add(t)
                }
            }
        } else {
            indexTypes = [defaultType]
        }
        log.debug("Assembled indexTypes: $indexTypes")
        return query(q, defaultIndex, indexTypes as String[])
    }

    SearchResult query(Query q, String indexName, String[] indexTypes, Class resultClass = searchResultClass) {
        log.trace "Doing query on $q"
        return query(q.toJsonQuery(), q.start, q.n, indexName, indexTypes, resultClass, q.highlights, q.facets)
    }

    SearchResult query(String jsonDsl, int start, int n, String indexName, String[] indexTypes, Class resultClass = searchResultClass, List highlights = null, List facets = null) {
        log.trace "Querying index $indexName and indextype $indexTypes"
        def idxlist = [indexName]
        if (indexName.contains(",")) {
            idxlist = indexName.split(",").collect{it.trim()}
        }
        log.trace("Searching in indexes: $idxlist")
        def response = client.search(new SearchRequest(idxlist as String[], jsonDsl.getBytes("utf-8")).searchType(SearchType.DFS_QUERY_THEN_FETCH).types(indexTypes)).actionGet()
        log.trace("SearchResponse: " + response)

        def results
        if (resultClass) {
            results = resultClass.newInstance()
        } else {
            results = new SearchResult()
        }
        results.numberOfHits = 0
        results.resultSize = 0
        results.startIndex = start
        results.searchCompletedInISO8601duration = "PT" + response.took.secondsFrac + "S"

        if (response) {
            results.resultSize = response.hits.hits.size()
            log.trace "Total hits: ${response.hits.totalHits}"
            results.numberOfHits = response.hits.totalHits
            response.hits.hits.each {
                if (highlights) {
                    results.addHit(createResultDocumentFromHit(it, indexName), convertHighlight(it.highlightFields))
                } else {
                    results.addHit(createResultDocumentFromHit(it, indexName))
                }
            }
            if (facets) {
                results.facets = convertFacets(response.facets.facets(), facets)
            }
        }
        return results
    }

    def Map<String, String[]> convertHighlight(Map<String, HighlightField> hfields) {
        def map = new TreeMap<String, String[]>()
        hfields.each {
            map.put(it.value.name, it.value.fragments)
        }
        return map
    }

    def convertFacets(eFacets, queryfacets) {
        def facets = new HashMap<String, Map<String, Integer>>()
        for (def f : eFacets) {
            def termcounts = [:]
            try {
                for (def entry : f.entries) {
                    termcounts[entry.term] = entry.count
                }
                facets.put(f.name, termcounts.sort { a, b -> b.value <=> a.value })
            } catch (MissingMethodException mme) {
                def group = queryfacets.facets.find {it.name == f.name}.group
                termcounts = facets.get(group, [:])
                if (f.count) {
                    termcounts[f.name] = f.count
                }
                facets.put(group, termcounts.sort { a, b -> b.value <=> a.value })
            }
        }
        return facets
    }

    Document createResultDocumentFromHit(hit, queriedIndex) {
        log.trace("creating document. ID: ${hit?.id}, index: $queriedIndex")
        return whelk.createDocument("application/json").withData(hit.source()).withIdentifier(fromElasticId(hit.id))
    }
    */

    public String getIndexName() { defaultIndex }
    public String getElasticHost() { elastichost.split(":").first() }
    public String getElasticCluster() { elasticcluster }
    public int getElasticPort() {
        try { new Integer(elastichost.split(",").first().split(":").last()).intValue() } catch (NumberFormatException nfe) { 9300 }
    }

    /**
     * ShapeComputer methods
     */

    String calculateTypeFromIdentifier(String id) {
        String identifier = new URI(id).path.toString()
        log.debug("Received uri $identifier")
        String idxType
        try {
            def identParts = identifier.split("/")
            idxType = (identParts[1] == whelk.id && identParts.size() > 3 ? identParts[2] : identParts[1])
        } catch (Exception e) {
            log.error("Tried to use first part of URI ${identifier} as type. Failed: ${e.message}")
        }
        if (!idxType) {
            idxType = defaultType
        }
        log.debug("Using type $idxType for ${identifier}")
        return idxType
    }

    String toElasticId(String id) {
        return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
    }

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

package whelk.component

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.*

import org.apache.commons.codec.binary.Base64

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.settings.*
import org.elasticsearch.search.highlight.*
import org.elasticsearch.action.admin.indices.flush.*
import org.elasticsearch.action.count.CountResponse
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.*
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.index.query.FilterBuilders.*
import org.elasticsearch.action.delete.*
import org.elasticsearch.action.get.*
import org.elasticsearch.action.search.*

import org.elasticsearch.common.io.stream.*
import org.elasticsearch.common.xcontent.*

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.common.xcontent.XContentFactory.*

import whelk.*
import whelk.plugin.*
import whelk.result.*
import whelk.exception.*

import static whelk.util.Tools.*

@Log
class ElasticSearchClient extends ElasticSearch implements Index {

    ElasticSearchClient(String ident = null, Map params) {
        super(params)
        id = ident
    }
}

@Log
abstract class ElasticSearch extends BasicComponent implements Index, ShapeComputer {

    static final String METAENTRY_INDEX_TYPE = "entry"
    static final String DEFAULT_CLUSTER = "whelks"
    static final int WARN_AFTER_TRIES = 1000
    static final int RETRY_TIMEOUT = 300
    static final int MAX_RETRY_TIMEOUT = 60*60*1000
    static final String DEFAULT_TYPE = "record"

    Client client
    String elastichost, elasticcluster
    String defaultType = "record"
    String defaultIndex = null

    Map<String,String> configuredTypes
    List<String> availableTypes


    Class searchResultClass = null

    ElasticSearch(Map settings) {
        this.elastichost = settings.get('elasticHost')
        if (!elastichost) {
            this.elastichost = System.getProperty("elastic.host")
        }
        this.elasticcluster = settings.get('elasticCluster')
        if (!elasticcluster) {
            this.elasticcluster = System.getProperty("elastic.cluster", DEFAULT_CLUSTER)
        }
        this.defaultType = settings.get("defaultType", DEFAULT_TYPE)
        connectClient()
        configuredTypes = (settings ? settings.get("typeConfiguration", [:]) : [:])
        availableTypes = (settings ? settings.get("availableTypes", []) : [])
        this.defaultIndex = (settings ? settings.get("indexName") : null)
        if (settings.searchResultClass) {
            this.searchResultClass = Class.forName(settings.searchResultClass)
        }
    }

    @Override
    void componentBootstrap(String whelkName) {
        if (!defaultIndex) {
            this.defaultIndex = whelkName
        }
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
                        client = client.addTransportAddress(new InetSocketTransportAddress(host, port as int))
                }
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                throw new WhelkRuntimeException("Unable to initialize elasticsearch client. Host configuration might be missing port?")
            }
            log.debug("... connected")
        } else {
            throw new WhelkRuntimeException("Unable to initialize ${this.id}. Need to configure plugins.json or set system property \"elastic.host\" and possibly \"elastic.cluster\".")
        }
    }

    def performExecute(def requestBuilder) {
        int failcount = 0
        def response = null
        while (response == null) {
            try {
                response = requestBuilder.execute().actionGet()
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

    void flush() {
        log.debug("Flusing ${this.id}")
        def flushresponse = performExecute(new FlushRequestBuilder(client.admin().indices()))
    }

    @Override
    void remove(String identifier, String dataset) {
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

            // Kanske en matchall-query filtrerad på _type och _id?
    }

    @Override
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
            log.debug("Decoded new style id into $decodedIdentifier")
            return decodedIdentifier
        }
    }
}

package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.settings.*
import org.elasticsearch.search.highlight.*
import org.elasticsearch.action.count.CountResponse
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.*

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*
import static org.elasticsearch.common.xcontent.XContentFactory.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

class ElasticSearchClientIndex extends ElasticSearchClient implements Index { }

@Log
class ElasticSearchClient extends ElasticSearch {

    // Force one-client-per-whelk
    ElasticSearchClient() {
        String elastichost, elasticcluster
        if (System.getProperty("elastic.host")) {
            elastichost = System.getProperty("elastic.host")
            elasticcluster = System.getProperty("elastic.cluster")
            log.info "Connecting to $elastichost:9300 using cluster $elasticcluster"
            def sb = ImmutableSettings.settingsBuilder()
            .put("client.transport.ping_timeout", 30000)
            .put("client.transport.sniff", true)
            if (elasticcluster) {
                sb = sb.put("cluster.name", elasticcluster)
            }
            Settings settings = sb.build();
            client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elastichost, 9300))
            log.debug("... connected")
        } else {
            throw new WhelkRuntimeException("Unable to initalize elasticsearch. Need at least system property \"elastic.host\" and possibly \"elastic.cluster\".")
        }
    }
}

@Log
class ElasticSearchNode extends ElasticSearch implements Index {

    ElasticSearchNode() {
        this(null)
    }

    ElasticSearchNode(String dataDir) {
        log.info "Starting elastic node"
        def elasticcluster = System.getProperty("elastic.cluster")
        ImmutableSettings.Builder sb = ImmutableSettings.settingsBuilder()
        if (elasticcluster) {
            sb = sb.put("cluster.name", elasticcluster)
        } else {
            sb = sb.put("cluster.name", "bundled_whelk_index")
        }
        if (dataDir != null) {
            sb.put("path.data", dataDir)
        }
        sb.build()
        Settings settings = sb.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        // start it!
        def node = nBuilder.build().start()
        client = node.client()
        log.info "Client connected to new (local) ES node."
    }
}

@Log
abstract class ElasticSearch extends BasicPlugin {

    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000
    int MAX_NUMBER_OF_FACETS = 100

    String URI_SEPARATOR = "::"

    String defaultIndexType = "record"

    @Override
    void init(String indexName) {
        if (!performExecute(client.admin().indices().prepareExists(indexName)).exists) {
            log.info("Creating index ...")
            XContentBuilder mapping = jsonBuilder().startObject()
            .startObject(indexName)
            .field("date_detection", false)
            .startObject("_timestamp")
            .field("enabled", true)
            .field("store", true)
            .endObject()
            .startObject("_source")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            log.debug("create: " + mapping.string())

            performExecute(client.admin().indices().prepareCreate(indexName).addMapping(defaultIndexType, mapping))
            setTypeMapping(indexName, defaultIndexType)
        }
    }

    @Override
    void delete(URI uri, String indexName) {
        log.debug("Deleting object with identifier $uri")
        def delQuery = termsQuery("_id", translateIdentifier(uri))
        log.debug("DelQuery: $delQuery")
        performExecute(client.prepareDeleteByQuery(indexName).setQuery(delQuery))
    }

    @Override
    void index(Document doc, String indexName) {
        if (doc) {
            addDocument(doc, indexType, indexName)
        }
    }

    @Override
    void bulkIndex(Iterable<Document> doc, String indexName) {
        addDocuments(doc, indexName)
    }

    @Override
    SearchResult query(Query q, String indexName) {
        def indexType = null
        if (q instanceof ElasticQuery) {
            indexType = q.indexType
        }
        return query(q, indexName, indexType)
    }


    SearchResult query(Query q, String indexName, String indexType) {
        log.debug "Querying index $indexName and indextype $indexType"
        log.trace "Doing query on $q"
        def idxlist = [indexName]
        if (indexName.contains(",")) {
            idxlist = indexName.split(",").collect{it.trim()}
        }
        log.debug("Searching in indexes: $idxlist")
        def srb = client.prepareSearch(idxlist as String[])
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFrom(q.start).setSize(q.n)
        if (indexType) {
            srb.setTypes(indexType.split(",") as String[])
        }
        if (q.query == "*") {
            log.debug("Setting matchAll")
            srb.setQuery(matchAllQuery())
        } else if (q.phraseQuery) {
            srb.setQuery(text(q.phraseField, q.phraseValue))
        } else {
            def query = queryString(q.query).defaultOperator(QueryStringQueryBuilder.Operator.AND)
            if (q.fields) {
                q.fields.each {
                    if (q.boost && q.boost[it]) {
                        query = query.field(it, q.boost[it])
                    } else {
                        query = query.field(it)
                    }
                }
            } else if (q.boost) {
                query = query.field("_all")
                q.boost.each { f, b ->
                    query = query.field(f, b)
                }
            }
            srb.setQuery(query)
        }
        if (q.sorting) {
            q.sorting.each {
                srb = srb.addSort(it.key, (it.value && it.value.equalsIgnoreCase('desc') ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC))
            }
        }
        if (q.highlights) {
            srb = srb.setHighlighterPreTags("").setHighlighterPostTags("")
            q.highlights.each {
                srb = srb.addHighlightedField(it)
            }
        }
        if (q.facets) {
            q.facets.each {
                if (it instanceof TermFacet) {
                    log.trace("Building FIELD facet for ${it.field}")
                    srb = srb.addFacet(FacetBuilders.termsFacet(it.name).field(it.field).size(MAX_NUMBER_OF_FACETS))
                } else if (it instanceof ScriptFieldFacet) {
                    if (it.field.contains("@")) {
                        log.warn("Forcing FIELD facet for ${it.field}")
                        srb = srb.addFacet(FacetBuilders.termsFacet(it.name).field(it.field).size(MAX_NUMBER_OF_FACETS))
                    } else {
                        log.trace("Building SCRIPTFIELD facet for ${it.field}")
                        srb = srb.addFacet(FacetBuilders.termsFacet(it.name).scriptField("_source.?"+it.field.replaceAll(/\./, ".?")).size(MAX_NUMBER_OF_FACETS))
                    }
                } else if (it instanceof QueryFacet) {
                    def qf = new QueryStringQueryBuilder(it.query).defaultOperator(QueryStringQueryBuilder.Operator.AND)
                    srb = srb.addFacet(FacetBuilders.queryFacet(it.name).query(qf))
                }
            }
        }
        log.debug("SearchRequestBuilder: " + srb)
        def response = performExecute(srb)
        log.trace("SearchResponse: " + response)

        def results = new SearchResult(0)

        if (response) {
            log.debug "Total hits: ${response.hits.totalHits}"
            results.numberOfHits = response.hits.totalHits
            response.hits.hits.each {
                if (q.highlights) {
                    results.addHit(createDocumentFromHit(it), convertHighlight(it.highlightFields))
                } else {
                    results.addHit(createDocumentFromHit(it))
                }
            }
            if (q.facets) {
                results.facets = convertFacets(response.facets.facets(), q)
            }
        }
        return results
    }

    def setTypeMapping(indexName, itype) {
        log.info("Creating mappings for $indexName/$itype ...")
        XContentBuilder mapping = jsonBuilder().startObject()
        .startObject(indexName)
        .field("date_detection", false)
        .field("store", true)
        .endObject()
        .endObject()
        log.debug("mapping: " + mapping.string())
        performExecute(client.admin().indices().preparePutMapping(indexName).setType(itype).setSource(mapping))
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

    void checkTypeMapping(indexName, indexType) {
        def mappings = performExecute(client.admin().cluster().prepareState()).state.metaData.index(indexName).getMappings()
        if (!mappings.containsKey(indexType)) {
            log.debug("Mapping for $indexType does not exist. Creating ...")
            setTypeMapping(indexName, indexType)
        }
    }

    String determineDocumentType(Document doc) {
        def idxType = doc.tags.find { it.type.toString() == "entityType"}?.value?.toLowerCase()
        if (!idxType) {
            try {
                idxType = doc.identifier.toString().split("/")[1]
            } catch (Exception e) {
                log.error("Tried to use first part of URI ${doc.identifier} as type. Failed: ${e.message}", e)
            }
        }
        if (!idxType) {
            idxType = defaultIndexType
        }
        log.debug("Using type $idxType for document ${doc.identifier}")
        return idxType
    }

    void addDocument(Document doc, String indexName) {
        addDocuments([doc], indexName)
    }


    void addDocuments(documents, indexName) {
        try {
            if (documents) {
                def breq = client.prepareBulk()

                def checkedTypes = [defaultIndexType]

                log.debug("Bulk request to index " + documents?.size() + " documents.")

                for (doc in documents) {
                    if (doc) {
                        def indexType = determineDocumentType(doc)
                        def checked = indexType in checkedTypes
                        if (!checked) {
                            checkTypeMapping(indexName, indexType)
                            checkedTypes << indexType
                        }
                        breq.add(client.prepareIndex(indexName, indexType, translateIdentifier(doc.identifier)).setSource(doc.data))
                    }
                }
                def response = performExecute(breq)
                if (response.hasFailures()) {
                    log.error "Bulk import has failures."
                    for (re in response.items) {
                        if (re.failed) {
                            log.error "Fail message for id ${re.id}, type: ${re.type}, index: ${re.index}: ${re.failureMessage}"
                            if (log.isTraceEnabled()) {
                                for (doc in documents) {
                                    if (doc.identifier.toString() == "/"+re.index+"/"+re.id) {
                                        log.trace("Failed document: ${doc.dataAsString}")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception thrown while adding documents", e)
            throw e
        }
    }

    def translateIdentifier(URI uri) {
        def pathparts = uri.path.split("/")
        def idelements = []
        pathparts.eachWithIndex() { part, i ->
            if (i > 0) {
                idelements.add(part)
            }
        }
        return idelements.join(URI_SEPARATOR)
    }

    def Map<String, String[]> convertHighlight(Map<String, HighlightField> hfields) {
        def map = new TreeMap<String, String[]>()
        hfields.each {
            map.put(it.value.name, it.value.fragments)
        }
        return map
    }

    def convertFacets(eFacets, query) {
        def facets = new HashMap<String, Map<String, Integer>>()
        for (def f : eFacets) {
            def termcounts = [:]
            try {
                for (def entry : f.entries) {
                    termcounts[entry.term] = entry.count
                }
                facets.put(f.name, termcounts.sort { a, b -> b.value <=> a.value })
            } catch (MissingMethodException mme) {
                def group = query.facets.find {it.name == f.name}.group
                termcounts = facets.get(group, [:])
                if (f.count) {
                    termcounts[f.name] = f.count
                }
                facets.put(group, termcounts.sort { a, b -> b.value <=> a.value })
            }
        }
        return facets
    }

    IndexDocument createDocumentFromHit(hit) {
        return new IndexDocument().withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id))
    }

    URI translateIndexIdTo(id) {
        return new URI("/"+id.replaceAll(URI_SEPARATOR, "/"))
    }

}

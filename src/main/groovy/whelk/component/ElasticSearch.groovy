package whelk.component

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.*

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
abstract class ElasticSearch extends BasicElasticComponent implements Index {

    String defaultType = "record"
    Map<String,String> configuredTypes
    List<String> availableTypes

    final static String INDEXNAME_SUFFIX = "_index"

    Class searchResultClass = null

    ElasticSearch(Map settings) {
        super(settings)
        configuredTypes = (settings ? settings.get("typeConfiguration", [:]) : [:])
        availableTypes = (settings ? settings.get("availableTypes", []) : [])
        if (settings.batchUpdateSize) {
            this.batchUpdateSize = settings.batchUpdateSize
        }
        if (settings.searchResultClass) {
            this.searchResultClass = Class.forName(settings.searchResultClass)
        }
    }

    @Override
    void componentBootstrap(String whelkName) {
        String indexName = whelkName + INDEXNAME_SUFFIX
        createIndexIfNotExists(indexName)
        flush()
        def realIndex = getRealIndexFor(indexName)
        availableTypes.each {
            checkTypeMapping(realIndex, it)
        }
    }

    String getIndexName() {
        return this.whelk.id + INDEXNAME_SUFFIX
    }

    String getLatestIndex(String prefix) {
        def indices = performExecute(client.admin().cluster().prepareState()).state.metaData.indices
        def li = new TreeSet<String>()
        for (idx in indices.keys()) {
            if (idx.value.startsWith(prefix+"-")) {
                li << idx.value
            }
        }
        log.debug("Latest index is ${li.last()}")
        return li.last()
    }

    String createNewCurrentIndex(String indexName) {
        assert (indexName != null)
        log.info("Creating index ...")
        es_settings = loadJson("es/es_settings.json")
        String currentIndex = "${indexName}-" + new Date().format("yyyyMMdd.HHmmss")
        log.debug("Will create index $currentIndex.")
        performExecute(client.admin().indices().prepareCreate(currentIndex).setSettings(es_settings))
        setTypeMapping(currentIndex, defaultType)
        availableTypes.each {
            setTypeMapping(currentIndex, it)
        }
        return currentIndex
    }

    void reMapAliases(String indexAlias) {
        String oldIndex = getRealIndexFor(indexAlias)
        String currentIndex = getLatestIndex(indexAlias)
        log.debug("Resetting alias \"$indexAlias\" from \"$oldIndex\" to \"$currentIndex\".")
        performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, indexAlias).removeAlias(oldIndex, indexAlias))
    }


    def loadJson(String file) {
        def json
        try {
            json = getClass().classLoader.getResourceAsStream(file).withStream {
                mapper.readValue(it, Map)
            }
        } catch (NullPointerException npe) {
            log.trace("File $file not found.")
        }
        return json
    }

    @Override
    void remove(String identifier) {
        String indexName = getIndexName()
        log.debug("Peforming deletebyquery to remove documents extracted from $identifier")
        def delQuery = termQuery("extractedFrom.@id", identifier)
        log.debug("DelQuery: $delQuery")

        def response = performExecute(client.prepareDeleteByQuery(indexName).setQuery(delQuery))

        log.debug("Delbyquery response: $response")
        for (r in response.iterator()) {
            log.debug("r: $r success: ${r.successfulShards} failed: ${r.failedShards}")
        }

        log.debug("Deleting object with identifier ${toElasticId(identifier)}.")

        client.delete(new DeleteRequest(indexName, calculateTypeFromIdentifier(identifier), toElasticId(identifier)))

            // Kanske en matchall-query filtrerad på _type och _id?
    }

    @Override
    SearchResult query(Query q) {
        String indexName = getIndexName()
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
        return query(q, indexName, indexTypes as String[])
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
        def metaEntryMap = null //getMetaEntry(hit.id, queriedIndex)
        if (metaEntryMap) {
            return whelk.createDocument(metaEntryMap?.contentType).withData(hit.source())
        } else {
            log.trace("Meta entry not found for document. Will assume application/json for content-type.")
            return whelk.createDocument("application/json").withData(hit.source()).withIdentifier(fromElasticId(hit.id))
        }
    }

    private Map getMetaEntry(id, queriedIndex) {
        //def emei = ".$queriedIndex"
        def emei = this.whelk.primaryStorage.indexName
        log.trace("Requested id: $id")
        id = fromElasticId(id)
        log.trace("Translated id: $id")
        try {
            def grb = new GetRequestBuilder(client, emei).setType(METAENTRY_INDEX_TYPE).setId(toElasticId(id))
            def result = performExecute(grb)
            if (result.exists) {
                return result.sourceAsMap
            }
        } catch (org.elasticsearch.indices.IndexMissingException ime) {
            log.debug("Meta entry index $emei does not exist.")
        }
        return null
    }

    public String getElasticHost() { elastichost }
    public String getElasticCluster() { elasticcluster }
    public int getElasticPort() { elasticport }
}

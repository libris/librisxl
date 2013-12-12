package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.*
import org.elasticsearch.common.transport.*
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.*
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

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*
import static org.elasticsearch.common.xcontent.XContentFactory.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*
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
            throw new WhelkRuntimeException("Unable to initialize elasticsearch. Need at least system property \"elastic.host\" and possibly \"elastic.cluster\".")
        }
    }
}

//TODO: Move all settings (general and index level) to config files and make creation of index and changing of settings to separate operation tasks

@Log
class ElasticSearchNode extends ElasticSearch implements Index {

    ElasticSearchNode() {
        this(null)
    }

    ElasticSearchNode(String dataDir) {
        log.info "Starting elastic node"
        def elasticcluster = System.getProperty("elastic.cluster")
        ImmutableSettings.Builder sb = ImmutableSettings.settingsBuilder()
        sb.put("node.name", "Parasite")
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

    def mapper

    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000
    int MAX_NUMBER_OF_FACETS = 100

    String URI_SEPARATOR = "::"

    String defaultIndexType = "record"
    String elasticIndex
    String currentIndex

    def defaultMapping, es_settings

    @Override
    void init(String indexName) {
        this.elasticIndex = indexName
        if (!performExecute(client.admin().indices().prepareExists(elasticIndex)).exists) {
            createNewCurrentIndex()
            log.debug("Will create alias $elasticIndex -> $currentIndex")
            performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, elasticIndex))
        } else {
            this.currentIndex = getRealIndexFor(elasticIndex)
            log.info("Using currentIndex: $currentIndex")
            if (this.currentIndex == null) {
                throw new WhelkRuntimeException("Unable to find a real current index for $elasticIndex")
            }
        }
    }

    String getRealIndexFor(String alias) {
        def aliases = performExecute(client.admin().cluster().prepareState()).state.metaData.aliases()
        log.debug("aliases: $aliases")
        def ri
        if (aliases.containsKey(alias)) {
            ri = aliases[alias]?.keySet().iterator().next()
        }
        return (ri ? ri : alias)
    }

    void createNewCurrentIndex() {
        log.info("Creating index ...")
        es_settings = loadJson("es_settings.json")
        this.currentIndex = "${elasticIndex}-" + new Date().format("yyyyMMdd.HHmmss")
        log.debug("Will create index $currentIndex.")
        performExecute(client.admin().indices().prepareCreate(currentIndex).setSettings(es_settings))
        setTypeMapping(currentIndex, defaultIndexType)
    }

    void reMapAliases() {
        def oldIndex = getRealIndexFor(elasticIndex)
        log.debug("Resetting alias \"$elasticIndex\" from \"$oldIndex\" to \"$currentIndex\".")
        performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, elasticIndex).removeAlias(oldIndex, elasticIndex))
    }

    void flush() {
        log.debug("Flushing indices.")
        def flushresponse = performExecute(new FlushRequestBuilder(client.admin().indices()))
        log.debug("Flush response: $flushresponse")
    }

    def loadJson(file) {
        def json
        mapper = mapper ?: new ObjectMapper()
        try {
            json = getClass().classLoader.getResourceAsStream(file).withStream {
                mapper.readValue(it, Map)
            }
        } catch (NullPointerException npe) {
            log.debug("File ${file.getName()} not found.")
        }
        return json
    }

    @Override
    void delete(URI uri) {
        log.debug("Deleting object with identifier $uri")
        def delQuery = termsQuery("_id", translateIdentifier(uri.toString()))
        log.debug("DelQuery: $delQuery")
        performExecute(client.prepareDeleteByQuery(elasticIndex).setQuery(delQuery))
    }

    @Override
    void index(IndexDocument doc) {
        if (doc) {
            addDocument(doc)
        }
    }

    @Override
    void bulkIndex(Iterable<IndexDocument> doc) {
        addDocuments(doc)
    }

    @Override
    SearchResult query(Query q) {
        def indexType = null
        if (q instanceof ElasticQuery) {
            indexType = q.indexType
        }
        return query(q, elasticIndex, indexType)
    }

    SearchResult query(Query q, String indexName, String indexType) {
        log.trace "Querying index $indexName and indextype $indexType"
        log.trace "Doing query on $q"
        def idxlist = [indexName]
        if (indexName.contains(",")) {
            idxlist = indexName.split(",").collect{it.trim()}
        }
        log.trace("Searching in indexes: $idxlist")
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
                def sortOrder =  it.value && it.value.equalsIgnoreCase('desc') ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC
                srb = srb.addSort(new FieldSortBuilder(it.key).order(sortOrder).missing("_last").ignoreUnmapped(true))
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
        log.trace("SearchRequestBuilder: " + srb)
        def response = performExecute(srb)
        log.trace("SearchResponse: " + response)

        def results = new SearchResult(0)

        if (response) {
            log.trace "Total hits: ${response.hits.totalHits}"
            results.numberOfHits = response.hits.totalHits
            response.hits.hits.each {
                if (q.highlights) {
                    results.addHit(createResultDocumentFromHit(it), convertHighlight(it.highlightFields))
                } else {
                    results.addHit(createResultDocumentFromHit(it))
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
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_")
        if (!defaultMapping) {
            defaultMapping = loadJson("default_mapping.json")
        }
        def typeMapping = loadJson("${itype}_mapping.json") ?: defaultMapping
        typeMapping.each { k, v ->
           mapping = mapping.field(k, v)
        }
        mapping = mapping.endObject().endObject()
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

    String determineDocumentType(IndexDocument doc) {
        def idxType = doc.type?.toLowerCase()
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
        log.trace("Using type $idxType for document ${doc.identifier}")
        return idxType
    }

    void addDocument(IndexDocument doc) {
        addDocuments([doc])
    }


    void addDocuments(documents) {
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
                            checkTypeMapping(currentIndex, indexType)
                            checkedTypes << indexType
                        }
                        breq.add(client.prepareIndex(currentIndex, indexType, translateIdentifier(doc.identifier)).setSource(doc.data))
                    }
                }
                def response = performExecute(breq)
                if (response.hasFailures()) {
                    log.error "Bulk import has failures."
                    def fails = []
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
                            try {
                                fails << translateIndexIdTo(re.id)
                            } catch (Exception e1) {
                                log.error("TranslateIndexIdTo cast an exception", e1)
                                fails << "Failed translation for \"$re\""
                            }
                        }
                    }
                    throw new WhelkAddException(fails)
                }
            }
        } catch (Exception e) {
            log.error("Exception thrown while adding documents", e)
            throw e
        }
    }

    def translateIdentifier(String uri) {
        def pathparts = new URI(uri).path.split("/")
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

    IndexDocument createResultDocumentFromHit(hit) {
        return new IndexDocument().withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id))
    }

    String translateIndexIdTo(id) {
        def pathelements = []
        id.split(URI_SEPARATOR).each {
            pathelements << java.net.URLEncoder.encode(it, "UTF-8")
        }
        return  new String("/"+pathelements.join("/"))
    }
}

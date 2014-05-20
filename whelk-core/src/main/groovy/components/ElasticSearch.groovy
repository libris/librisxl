package se.kb.libris.whelks.component

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

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

@Log
class ElasticSearchClient extends ElasticSearch implements Index {

    // Force one-client-per-whelk
    ElasticSearchClient(Map params) {
        super(params)
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
abstract class ElasticSearch extends BasicComponent implements Index {

    final static ObjectMapper mapper = new ObjectMapper()

    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000
    static int MAX_NUMBER_OF_FACETS = 100

    String URI_SEPARATOR = "::"

    String defaultType = "record"
    Map<String,String> configuredTypes
    ElasticShapeComputer shapeComputer

    def defaultMapping, es_settings

    ElasticSearch(Map settings) {
        configuredTypes = (settings ? settings.get("typeConfiguration", [:]) : [:])
        if (settings.batchUpdateSize) {
            this.batchUpdateSize = settings.batchUpdateSize
        }
    }

    @Override
    void init(String indexName) {
        super.init(indexName)
        createIndexIfNotExists(indexName)
        shapeComputer = plugins.find { it instanceof ElasticShapeComputer }
    }

    void createIndexIfNotExists(String indexName) {
        if (!performExecute(client.admin().indices().prepareExists(indexName)).exists) {
            log.info("Couldn't find index by name $indexName. Creating ...")
            if (indexName.startsWith(".")) {
                // It's a meta index. No need for aliases and such.
                if (!es_settings) {
                    es_settings = loadJson("es_settings.json")
                }
                performExecute(client.admin().indices().prepareCreate(indexName).setSettings(es_settings))
            } else {
                String currentIndex = createNewCurrentIndex(indexName)
                log.debug("Will create alias $indexName -> $currentIndex")
                performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, indexName))
            }
        } else if (getRealIndexFor(indexName) == null) {
            throw new WhelkRuntimeException("Unable to find a real current index for $indexName")
        }
    }

    String getRealIndexFor(String alias) {
        def aliases = performExecute(client.admin().cluster().prepareState()).state.metaData.aliases()
        log.debug("aliases: $aliases")
        def ri = null
        if (aliases.containsKey(alias)) {
            ri = aliases.get(alias)?.keys().iterator().next()
        }
        if (ri) {
            log.trace("ri: ${ri.value} (${ri.value.getClass().getName()})")
        }
        return (ri ? ri.value : alias)
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
        es_settings = loadJson("es_settings.json")
        String currentIndex = "${indexName}-" + new Date().format("yyyyMMdd.HHmmss")
        log.debug("Will create index $currentIndex.")
        performExecute(client.admin().indices().prepareCreate(currentIndex).setSettings(es_settings))
        setTypeMapping(currentIndex, defaultType)
        return currentIndex
    }

    void reMapAliases(String indexAlias) {
        String oldIndex = getRealIndexFor(indexAlias)
        String currentIndex = getLatestIndex(indexAlias)
        log.debug("Resetting alias \"$indexAlias\" from \"$oldIndex\" to \"$currentIndex\".")
        performExecute(client.admin().indices().prepareAliases().addAlias(currentIndex, indexAlias).removeAlias(oldIndex, indexAlias))
    }

    void flush() {
        log.debug("Flushing indices.")
        def flushresponse = performExecute(new FlushRequestBuilder(client.admin().indices()))
        log.debug("Flush response: $flushresponse")
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

    void deleteEntry(URI uri, indexName) {
        client.delete(new DeleteRequest(indexName, "entry", translateIdentifier(uri.toString())))
    }

    @Override
    void remove(URI uri) {
        String indexName = this.whelk.id
        log.debug("Peforming deletebyquery to remove documents extracted from $uri")
        def delQuery = termQuery("extractedFrom.@id", uri.toString())
        log.debug("DelQuery: $delQuery")

        def response = performExecute(client.prepareDeleteByQuery(indexName).setQuery(delQuery))

        log.debug("Delbyquery response: $response")
        for (r in response.iterator()) {
            log.debug("r: $r success: ${r.successfulShards} failed: ${r.failedShards}")
        }

        log.debug("Deleting object with identifier ${translateIdentifier(uri.toString())}.")

        client.delete(new DeleteRequest(indexName, shapeComputer.calculateShape(uri), translateIdentifier(uri.toString())))

        setState(LAST_UPDATED, new Date().getTime())
            // Kanske en matchall-query filtrerad på _type och _id?
    }

    @Override
    void index(Document doc) {
        String indexName = this.whelk.id
        if (doc && doc.isJson()) {
            addDocuments([doc], indexName)
        }
    }

    Document get(URI uri) {
        throw new UnsupportedOperationException("Not implemented yet.")
    }

    @Override
    protected void batchLoad(List<Document> docs) {
        String indexName = this.whelk.id
        addDocuments(docs, indexName)
    }

    @Override
    InputStream rawQuery(String query) {

    }

    @Override
    SearchResult query(Query q) {
        String indexName = this.whelk.id
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

    SearchResult query(Query q, String indexName, String[] indexTypes) {
        log.trace "Querying index $indexName and indextype $indexTypes"
        log.trace "Doing query on $q"
        def idxlist = [indexName]
        if (indexName.contains(",")) {
            idxlist = indexName.split(",").collect{it.trim()}
        }
        log.trace("Searching in indexes: $idxlist")
        def jsonDsl = q.toJsonQuery()
        def response = client.search(new SearchRequest(idxlist as String[], jsonDsl.getBytes("utf-8")).searchType(SearchType.DFS_QUERY_THEN_FETCH).types(indexTypes)).actionGet()
        log.trace("SearchResponse: " + response)

        def results = new SearchResult(0)

        if (response) {
            log.trace "Total hits: ${response.hits.totalHits}"
            results.numberOfHits = response.hits.totalHits
            response.hits.hits.each {
                if (q.highlights) {
                    results.addHit(createResultDocumentFromHit(it, indexName), convertHighlight(it.highlightFields))
                } else {
                    results.addHit(createResultDocumentFromHit(it, indexName))
                }
            }
            if (q.facets) {
                results.facets = convertFacets(response.facets.facets(), q)
            }
        }
        return results
    }

    /*
    Iterator<String> oldmetaEntryQuery(String indexName, String dataset, Date since, Date until) {
        def query = boolQuery()
        if (dataset) {
            query = query.must(termQuery("entry.dataset", dataset))
        }
        if (since || until) {
            def timeRangeQuery = rangeQuery("entry.timestamp")
            if (since) {
                timeRangeQuery = timeRangeQuery.from(since.getTime())
            }
            if (until) {
                timeRangeQuery = timeRangeQuery.to(since.getTime())
            }
            query = query.must(timeRangeQuery)
        }
        def srb = client.prepareSearch(indexName)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setScroll(new TimeValue(60000))
            .setTypes(["entry"] as String[])
            .setQuery(query)
            .addSort("entry.timestamp", SortOrder.ASC)
            .setSize(100)

        def list = []
        log.debug("MetaEntryQuery: $srb")
        def scrollResp = performExecute(srb)
        return new Iterator<String>() {
            public boolean hasNext() {
                if (list.size() == 0) {
                    scrollResp = performExecute(super.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)))
                    //list.addAll(scrollResp.hits.hits.collect { translateIndexIdTo(it.id) })
                    for (hit in scrollResp.hits.hits) {
                        list.add(hit.id)
                        log.info("doc timestamp: " + mapper.readValue(hit.source(), Map).entry.timestamp)
                    }
                }
                return list.size()
            }
            public String next() { list.pop() }
            public void remove() { throw new UnsupportedOperationException(); }
        }
    }
*/

    long loadHighestSequenceNumber(String indexName) {
        long sequenceNumber = 0L
        def srb = client.prepareSearch(indexName).setTypes(["entry"] as String[]).setFrom(0).setSize(1).setQuery(matchAllQuery())
            .addSort(new FieldSortBuilder("entry.sequenceNumber").ignoreUnmapped(true).missing(0L).order(SortOrder.DESC))
        def response = performExecute(srb)
        log.debug("Total number of sequence number hits: ${response.hits.totalHits}")
        if (response.hits.totalHits > 0) {
            log.debug("Found matches for documentSequenceNumber")
            sequenceNumber = mapper.readValue(response.hits.getAt(0).source(), Map).entry.get("sequenceNumber", 0L)
        }
        log.debug("Highest documentNumber: $sequenceNumber")
        return sequenceNumber
    }

    private SearchRequestBuilder buildMetaEntryQuery(String indexName, String dataset, Date since, Date until, long lastTimestamp = -1, long lastSequence = -1) {
        def query = boolQuery()
        if (dataset) {
            query = query.must(termQuery("entry.dataset", dataset))
        }
        if (lastTimestamp < 0 && (since || until)) {
            def timeRangeQuery = rangeQuery("_timestamp")
            if (since) {
                timeRangeQuery = timeRangeQuery.from(since.getTime())
            }
            if (until) {
                timeRangeQuery = timeRangeQuery.to(since.getTime())
            }
            query = query.must(timeRangeQuery)
        }
        if (lastTimestamp >= 0 && lastSequence >= 0) {
            def tsQuery = rangeQuery("_timestamp").gte(lastTimestamp)
            query = query.must(tsQuery)
            if (lastSequence > 0) {
                def snQuery = rangeQuery("entry.sequenceNumber").gt(lastSequence)
                query = query.must(snQuery)
            }
        }

        def srb = client.prepareSearch(indexName)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setTypes(["entry"] as String[])
            .setFetchSource(["identifier", "entry.sequenceNumber"] as String[], null)
            .addField("_timestamp")
            .setQuery(query)
            .setFrom(0).setSize(100)
            .addSort("_timestamp", SortOrder.ASC)
            .addSort(new FieldSortBuilder("entry.sequenceNumber").ignoreUnmapped(true).missing(0L).order(SortOrder.ASC))

        log.debug("MetaEntryQuery: $srb")

        return srb
    }

    Iterator<String> metaEntryQuery(String indexName, String dataset, Date since, Date until) {
        LinkedList<String> list = new LinkedList<String>()
        long lastDocumentTimestamp = -1L
        long documentSequenceNumber = 0L

        return new Iterator<String>() {
            String lastLoadedIdentifier = null

            public boolean hasNext() {
                if (list.size() == 0) {
                    def srb = buildMetaEntryQuery(indexName, dataset, since, until, lastDocumentTimestamp, documentSequenceNumber)
                    InputStream inputStream = new PipedInputStream()
                    OutputStream outputStream = new PipedOutputStream(inputStream)

                    def response = performExecute(srb)

                    String ident = null

                    for (hit in response.hits.hits) {
                        lastDocumentTimestamp = hit.field("_timestamp").value
                        documentSequenceNumber = mapper.readValue(hit.source(), Map).entry.get("sequenceNumber", 0L)
                        ident = translateIndexIdTo(hit.id)
                        list.add(ident)
                    }
                    if (lastLoadedIdentifier && lastLoadedIdentifier == ident) {
                        log.warn("Got the identifier (${lastLoadedIdentifier}) again. Pulling the plug!!")
                        return false
                    }
                    lastLoadedIdentifier = ident
                }
                return list.size()
            }
            public String next() { list.pop() }
            public void remove() { throw new UnsupportedOperationException(); }
        }
    }

    private void setTypeMapping(indexName, itype) {
        log.info("Creating mappings for $indexName/$itype ...")
        //XContentBuilder mapping = jsonBuilder().startObject().startObject("mappings")
        if (!defaultMapping) {
            defaultMapping = loadJson("default_mapping.json")
        }
        def typePropertyMapping = loadJson("${itype}_mapping_properties.json")
        def typeMapping
        if (typePropertyMapping) {
            log.debug("Found properties mapping for $itype. Using them with defaults.")
            typeMapping = new HashMap(defaultMapping)
            typeMapping.put("properties", typePropertyMapping.get("properties"))
        } else {
            typeMapping = loadJson("${itype}_mapping.json") ?: defaultMapping
        }
        // Append special mapping for @id-fields
        if (!typeMapping.dynamic_templates) {
            typeMapping['dynamic_templates'] = []
        }
        if (!typeMapping.dynamic_templates.find { it.containsKey("id_template") }) {
            log.debug("Found no id_template. Creating.")
            typeMapping.dynamic_templates << ["id_template":["match":"@id","match_mapping_type":"string","mapping":["type":"string","index":"not_analyzed"]]]
        }

        String mapping = mapper.writeValueAsString(typeMapping)
        log.debug("mapping for $indexName/$itype: " + mapping)
        def response = performExecute(client.admin().indices().preparePutMapping(indexName).setType(itype).setSource(mapping))
        log.debug("mapping response: ${response.acknowledged}")
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
        log.debug("Checking mappings for index $indexName, type $indexType")
        def mappings = performExecute(client.admin().cluster().prepareState()).state.metaData.index(indexName).getMappings()
        log.trace("Mappings: $mappings")
        if (!mappings.containsKey(indexType)) {
            log.debug("Mapping for $indexName/$indexType does not exist. Creating ...")
            setTypeMapping(indexName, indexType)
        }
    }

    String determineDocumentType(Document doc, String indexName) {
        def idxType = doc.entry['dataset']?.toLowerCase()
        log.trace("dataset in entry is ${idxType} for ${doc.identifier}")
        if (!idxType) {
            idxType = shapeComputer.calculateShape(doc.identifier)
        }
        log.trace("Using type $idxType for document ${doc.identifier}")
        return idxType
    }

    void index(final List<Map<String,String>> data) throws WhelkIndexException  {
        def breq = client.prepareBulk()
        for (entry in data) {
            breq.add(client.prepareIndex(entry['index'], entry['type'], entry['id']).setSource(entry['data'].getBytes("utf-8")))
        }
        def response = performExecute(breq)
        if (response.hasFailures()) {
            log.error "Bulk entry indexing has failures."
            def fails = []
            for (re in response.items) {
                if (re.failed) {
                    log.error "Fail message for id ${re.id}, type: ${re.type}, index: ${re.index}: ${re.failureMessage}"
                    try {
                        fails << translateIndexIdTo(re.id)
                    } catch (Exception e1) {
                        log.error("TranslateIndexIdTo cast an exception", e1)
                        fails << "Failed translation for \"$re\""
                    }
                }
            }
            throw new WhelkIndexException("Failed to index entries. Reason: ${response.buildFailureMessage()}", new WhelkAddException(fails))
        } else {
            log.debug("Direct bulk request completed in ${response.tookInMillis} millseconds.")
        }
    }

    void index(byte[] data, Map params) throws WhelkIndexException  {
        try {
            def response = performExecute(client.prepareIndex(params['index'], params['type'], params['id']).setSource(data))
            log.debug("Raw byte indexer (${params.index}/${params.type}/${params.id}) indexed version: ${response.version}")
        } catch (Exception e) {
            throw new WhelkIndexException("Failed to index ${new String(data)} with params $params", e)
        }
    }

    void addDocuments(documents, indexName) {
        String currentIndex = getRealIndexFor(indexName)
        log.debug("Using $currentIndex for indexing.")
        try {
            if (documents) {
                def breq = client.prepareBulk()

                def checkedTypes = [defaultType]

                log.debug("Bulk request to index " + documents?.size() + " documents.")

                for (doc in documents) {
                    log.trace("Working on ${doc.identifier}")
                    if (doc && doc.isJson()) {
                        //def indexType = determineDocumentType(doc, indexName)
                        def indexType = shapeComputer.calculateShape(doc.identifier)
                        def checked = indexType in checkedTypes
                        if (!checked) {
                            checkTypeMapping(currentIndex, indexType)
                            checkedTypes << indexType
                        }
                        def elasticIdentifier = translateIdentifier(doc.identifier)
                        breq.add(client.prepareIndex(indexName, indexType, elasticIdentifier).setSource(doc.data))
                    } else {
                        log.debug("Doc is null or not json (${doc.contentType})")
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

    String translateIdentifier(String uri) {
        def idelements = new URI(uri).path.split("/") as List
        idelements.remove(0)
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

    Document createResultDocumentFromHit(hit, queriedIndex) {
        log.trace("creating document. ID: ${hit?.id}, index: $queriedIndex")
        def metaEntryMap = getMetaEntry(hit.id, queriedIndex)
        if (metaEntryMap) {
            return new Document(metaEntryMap).withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id))
        } else {
            log.trace("Meta entry not found for document. Will assume application/json for content-type.")
            return new Document().withData(hit.source()).withContentType("application/json").withIdentifier(translateIndexIdTo(hit.id))
        }
    }

    private Map getMetaEntry(id, queriedIndex) {
        def emei = ".$queriedIndex"
        try {
            def grb = new GetRequestBuilder(client, emei).setType("entry").setId(id)
            def result = performExecute(grb)
            if (result.exists) {
                return result.sourceAsMap
            }
        } catch (org.elasticsearch.indices.IndexMissingException ime) {
            log.debug("Meta entry index $emei does not exist.")
        }
        return null
    }



    String translateIndexIdTo(id) {
        def pathelements = []
        id.split(URI_SEPARATOR).each {
            pathelements << java.net.URLEncoder.encode(it, "UTF-8")
        }
        return  new String("/"+pathelements.join("/"))
    }
}

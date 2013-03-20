package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import groovy.transform.Synchronized

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.get.GetResponse
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

import org.json.simple.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

@Log
abstract class ElasticSearch extends BasicPlugin {

    //String idxpfx
    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000
    int MAX_NUMBER_OF_FACETS = 100

    String URI_SEPARATOR = "::"

    String indexType = "record"
    String indexMetadataType = "metadata"
    String storageType = "document"

    @Override
    void index(Document doc, String idxpfx) {
        if (doc) {
            addDocument(doc, indexType, idxpfx)
        }
    }

    @Override
    void index(Iterable<Document> doc, String idxpfx) {
        addDocuments(doc, indexType, idxpfx)
    }

    @Override
    void delete(URI uri, String idxpfx) {
        log.debug("Deleting object with identifier $uri")
        performExecute(client.prepareDelete(idxpfx, indexType, translateIdentifier(uri)))
        performExecute(client.prepareDelete(idxpfx, storageType, translateIdentifier(uri)))
    }

    @Override
    public void store(Document doc, String idxpfx) {
        addDocument(doc, storageType, idxpfx)
    }

    @Override
    public void store(Iterable<Document> doc, String idxpfx) {
        addDocuments(doc, storageType, idxpfx)
    }

    @Override
    Document get(URI uri, String idxpfx) {
        log.debug("Received GET request for $uri")
        GetResponse response = performExecute(client.prepareGet(idxpfx, storageType, translateIdentifier(uri)).setFields("_source","_timestamp"))
        if (response && response.exists()) {
            def ts = (response.field("_timestamp") ? response.field("_timestamp").value : null)
            try {
                return new BasicDocument(response.sourceAsString())
            } catch (DocumentException de) {
                log.error("Failed to created document with uri ${uri} from source - " + de.getMessage(), de)
            }
        }
        return null
    }

    @Override
    Iterable<Document> getAll(String idxpfx) {
        return new ElasticIterable<Document>(this, idxpfx)
    }

    @Override
    void init(String idxpfx) {
        if (!performExecute(client.admin().indices().prepareExists(idxpfx)).exists()) {
            log.info("Creating index ...")
            XContentBuilder mapping = jsonBuilder().startObject()
            .startObject(idxpfx)
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

            performExecute(client.admin().indices().prepareCreate(idxpfx).addMapping(indexType, mapping))
            setTypeMapping(idxpfx, indexType)
        }
    }

    def setTypeMapping(idxpfx, itype) {
        log.info("Creating mappings for $idxpfx/$itype ...")
        XContentBuilder mapping = jsonBuilder().startObject()
        .startObject(idxpfx)
        .field("date_detection", false)
        .field("store", true)
        .endObject()
        .endObject()
        log.debug("mapping: " + mapping.string())
        performExecute(client.admin().indices().preparePutMapping(idxpfx).setType(itype).setSource(mapping))
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

    void checkTypeMapping(idxpfx, entityType) {
        def mappings = performExecute(client.admin().cluster().prepareState()).state().getMetaData().index(idxpfx).getMappings()
        if (!mappings.containsKey(entityType)) {
            log.debug("Mapping for $entityType does not exist. Creating ...")
            setTypeMapping(idxpfx, entityType)
        }
    }


    void addDocument(Document doc, String addType, String idxpfx) {
        def eid = translateIdentifier(doc.identifier)
        def entityType = doc.tags.find { it.type.toString() == "entityType"}?.value ?: addType
        // Check if 
        if (entityType != indexType) {
            checkTypeMapping(idxpfx, entityType)
        }

        log.trace "Should use index ${idxpfx}, type ${entityType} and id ${eid}"
        try {
            def irb = client.prepareIndex(idxpfx, entityType, eid)
            if (addType == indexType) {
                irb.setSource(doc.data)
            } else {
                irb.setTimestamp(""+doc.getTimestamp()).setSource(doc.toJson())
            }
            IndexResponse response = performExecute(irb)
            log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
            log.trace("Prepareing metadata indexing with type $indexMetadataType and metadatajson: " + doc.getMetadataJson())
            irb = client.prepareIndex(idxpfx, indexMetadataType, eid).setSource(doc.getMetadataJson())
            response = performExecute(irb)
        } catch (org.elasticsearch.index.mapper.MapperParsingException me) {
            log.error("Failed to index document with id ${doc.identifier}: " + me.getMessage(), me)
        }
    }

    void addDocuments(documents, addType, idxpfx) {
        try {
            if (documents) {
                def breq = client.prepareBulk()

                log.debug("Bulk request to index " + documents?.size() + " documents.")

                for (doc in documents) {
                    if (addType == indexType) {
                        breq.add(client.prepareIndex(idxpfx, addType, translateIdentifier(doc.identifier)).setSource(doc.data))
                        log.debug("Prepareing index (bulk) of type $indexMetadataType with metadatajson: " + doc.getMetadataJson())
                        breq.add(client.prepareIndex(idxpfx, indexMetadataType, translateIdentifier(doc.identifier)).setSource(doc.getMetadataJson()))
                    } else {
                        breq.add(client.prepareIndex(idxpfx, addType, translateIdentifier(doc.identifier)).setSource(doc.toJson()))
                    }
                }
                def response = performExecute(breq)
                if (response.hasFailures()) {
                    log.error "Bulk import has failures."
                    for (def re : response.items()) {
                        if (re.failed()) {
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

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticSearch.this.addDocument(doc, storageType)
                }
            }
    }

    def translateIdentifier(URI uri) {
        def pathparts = uri.path.split("/")
        def idelements = []
        pathparts.eachWithIndex() { part, i ->
            if (i > 1) {
                idelements.add(part)
            }
        }
        return idelements.join(URI_SEPARATOR)
    }

    URI translateIndexIdTo(id, idxpfx) {
        return new URI("/"+idxpfx+"/"+id.replaceAll(URI_SEPARATOR, "/"))
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
                for (def entry : f.entries()) {
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

    @Override
    SearchResult query(Query q, String idxpfx, String indexType) {
        def iType = (indexType == null ? [this.indexType] : indexType.split(","))
        log.debug "Querying index $idxpfx and indextype $iType"
        log.trace "Doing query on $q"
        def idxlist = [idxpfx]
        if (idxpfx.contains(",")) {
            idxlist = idxpfx.split(",").collect{it.trim()}
        }
        log.debug("Searching in indexes: $idxlist")
        def srb = client.prepareSearch(idxlist as String[]).setTypes(iType as String[])
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFrom(q.start).setSize(q.n)
        if (q.query == "*") {
            log.debug("Setting matchAll")
            srb.setQuery(matchAllQuery())
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
                    srb = srb.addFacet(FacetBuilders.termsFacet(it.name).field(it.field).size(MAX_NUMBER_OF_FACETS))
                    // TODO: Figure out why this makes the query crash sometimes
                    //srb = srb.addFacet(FacetBuilders.termsFacet(it.name).scriptField("_source."+it.field).size(MAX_NUMBER_OF_FACETS))
                } else if (it instanceof QueryFacet) {
                    def qf = new QueryStringQueryBuilder(it.query).defaultOperator(QueryStringQueryBuilder.Operator.AND)
                    srb = srb.addFacet(FacetBuilders.queryFacet(it.name).query(qf))
                }
            }
        }
        log.trace("SearchRequestBuilder: " + srb)
        def response = performExecute(srb)
        log.trace("SearchResponse: " + response)

        def results = new BasicSearchResult(0)

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

    SearchResult query(Query q, String idxpfx) {
       return query(q, idxpfx, indexType)
    }

    Document createDocumentFromHit(hit) {
        return new BasicDocument().withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id, hit.index))
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    def Iterable<Document> updates(Date since) {
        return new ElasticIterable<Document>(this, since, true)
    }

    def loadAll(String idxpfx, String token = null, Date since = null, boolean loadDocuments = true, boolean sorted=false) {
        def results
        if (loadDocuments) {
            results = new ArrayList<Document>()
        } else {
            results = new ArrayList<LogEntry>()
        }
        def srb
        if (!token) {
            log.trace("Starting matchAll-query")
            srb = client.prepareSearch(idxpfx)
            if (loadDocuments) {
                srb = srb.addField("_source")
            }
            srb = srb.setTypes(storageType)
                .setScroll(TimeValue.timeValueMinutes(20))
                .setSize(History.BATCH_SIZE)
            if (sorted) {
                def query
                if (since) {
                    query = rangeQuery("_timestamp").gte(since.getTime())
                } else {
                    query = matchAllQuery()
                }
                srb = srb.addField("_timestamp")
                    .addSort("_timestamp", org.elasticsearch.search.sort.SortOrder.ASC)
                    .setQuery(query)
            } else {
                srb.setQuery(matchAllQuery())
            }
        } else {
            log.trace("Continuing query with scrollId $token")
            srb = client.prepareSearchScroll(token).setScroll(TimeValue.timeValueMinutes(2))
        }
        log.trace("loadAllquery: " + srb)
        def response = performExecute(srb)
        log.trace("Response: " + response)
        if (response.timedOut()) {
            log.warn("Response timed out")
        }
        if (response) {
            log.trace "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each {
                if (loadDocuments) {
                    try {
                        results.add(new BasicDocument(new String(it.source())))
                    } catch (DocumentException de) {
                        log.error("Failed to created document with id ${it.id} from source - " + de.getMessage(), de)
                    }
                } else {
                    results.add(new LogEntry(translateIndexIdTo(it.id, it.index), new Date(it.field("_timestamp").value)))
                }
            }
        } else if (!response || response.hits.length < 1) {
            log.info("No response recevied.")
        }
        log.debug("Found " + results.size() + " items. Scroll ID: " + response.scrollId())
        return [results, response.scrollId()]
    }
}


@Log 
class ElasticIterable<T> implements Iterable {
    def indexInstance
    Collection<T> list
    boolean incomplete = false
    boolean sorted
    def token
    String idxpfx
    Date since

    ElasticIterable(Index idx, String idxpfx, Date snc = null, boolean srt = false) {
        log.debug("Creating new iterable.")
        indexInstance = idx
        this.idxpfx = idxpfx
        since = snc
        sorted = srt
        (list, token) = indexInstance.loadAll(idxpfx, null, since, true, sorted)
        log.debug("Initial list with size: ${list.size} and token: $token")
        incomplete = (list.size == History.BATCH_SIZE)
    }

    Iterator<T> iterator() {
        return new ElasticIterator<T>()
    }

    class ElasticIterator<T> implements Iterator {

        Iterator iter

        ElasticIterator() {
            iter = list.iterator()
        }

        boolean hasNext() {
            return iter.hasNext()
        }

        @Synchronized
        T next() {
            T n = iter.next();
            iter.remove();
            if (!iter.hasNext() && incomplete) {
               refill()
            }
            return n
        }

        void remove() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Synchronized
        private void refill() {
            (list, token) = this.indexInstance.loadAll(this.idxpfx, token, since, true, sorted)
            incomplete = (list.size() == History.BATCH_SIZE)
            iter = list.iterator()
        }
    }
}

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

class ElasticSearchClientStorage extends ElasticSearchClient implements Storage { }
class ElasticSearchClientIndex extends ElasticSearchClient implements Index { }
class ElasticSearchClientHistory extends ElasticSearchClient implements History { }
class ElasticSearchClientIndexHistory extends ElasticSearchClient implements Index, History { }
class ElasticSearchClientStorageIndexHistory extends ElasticSearchClient implements Storage, Index, History { }


@Log
class ElasticSearchNode extends ElasticSearch implements Index {

    ElasticSearchNode() {
        this(null)
    }

    ElasticSearchNode(String dataDir) {
        log.debug "Creating elastic node"
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
        log.debug "Client connected to new ES node"
    }

}

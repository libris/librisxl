package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

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
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*
import static org.elasticsearch.common.xcontent.XContentFactory.*

import org.json.simple.*
import com.google.gson.*
import groovy.json.JsonSlurper

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*

import static se.kb.libris.conch.Tools.*

@Log
abstract class ElasticSearch implements Index, Storage, History {

    String index
    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int WARN_AFTER_TRIES = 1000
    int RETRY_TIMEOUT = 300
    int MAX_RETRY_TIMEOUT = 60*60*1000

    String URI_SEPARATOR = "::"

    String indexType = "record"
    String storageType = "document"

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    def init() {
        int failcount = 0
        while (true) {
            try {
                if (!client.admin().indices().prepareExists(index).execute().actionGet().exists()) {
                    log.debug("Creating index ...")
                    XContentBuilder mapping = jsonBuilder().startObject()
                    .startObject(index)
                    .startObject("_timestamp")
                    .field("enabled", true)
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    log.debug("mapping: " + mapping.string())

                    client.admin().indices().prepareCreate(index).addMapping(storageType, mapping).execute()
                }
                break;
            } catch (NoNodeAvailableException nnae) {
                log.debug("Retrying elasticsearch connection ...")
                if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to store document after $failcount attempts.")
                }
                Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
            }
        }
        /*
        def imd = client.admin().cluster().prepareState().setFilterIndices(index).execute().actionGet().getState().getMetaData().index(index)
        def types = []
        for (def mmd : imd.mappings) {
            println "mmd: " + mmd
            //types.add(mmd.type())
        }
        println "Types: " + types
        */
    }

    def boolean add(Document doc, String addType) {
        boolean unfailure = false
        def eid = translateIdentifier(doc.identifier)
        log.debug "Should use index ${index}, type ${addType} and id ${eid}"
        try {
            def irb = client.prepareIndex(index, addType, eid)
            if (addType == indexType) {
                irb.setSource(doc.data)
            } else {
                irb.setTimestamp(""+doc.getTimestamp()).setSource(serializeDocumentToJson(doc))
            }
            IndexResponse response = irb.execute().actionGet()
            unfailure = true
            log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        } catch (org.elasticsearch.index.mapper.MapperParsingException me) {
            log.error("Failed to index document with id ${doc.identifier}: " + me.getMessage(), me)
            unfailure = true
        } catch (org.elasticsearch.client.transport.NoNodeAvailableException nnae) {
            log.trace("Failed to connect to elasticsearch node: " + nnae.getMessage(), nnae)
        }
        return unfailure
    }

    void addLoop(Document doc, String type) {
        int failcount = 0
        while (!add(doc, type)) {
            if (failcount++ > WARN_AFTER_TRIES) {
                log.warn("Failed to store document after $failcount attempts.")
            }
            Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
        }
    }

    @Override
    void index(Document doc) {
        addLoop(doc, indexType)
    }

    void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    public void store(Document doc) {
        addLoop(doc, storageType)
    }

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticSearch.this.add(doc, storageType)
                }
            }
    }

    boolean isJSON(data) {
        Gson gson = new Gson()
        try {
            gson.fromJson(new String(data), Object.class)
        } catch (JsonSyntaxException jse) {
            log.debug("Data was not appliction/json")
            return false
        }
        return true
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

    URI translateIndexIdTo(id) {
        return new URI("/"+index+"/"+id.replaceAll(URI_SEPARATOR, "/"))
    }

    /*
    def determineIndexAndType(URI uri, String fallbackType) {
        log.debug "uripath: ${uri.path}"
        def pathparts = uri.path.split("/").reverse()
        int maxpart = pathparts.size() - 2
        def dict = [:]
        def typeelements = []
        pathparts.eachWithIndex() { part, i ->
            if (i == 0) {
                dict['id'] = part
            } else if (i == maxpart) {
                dict['index'] = part
            } else if (i < maxpart) {
                typeelements.add(part)
            }
        }
        if (!dict['index']) {
            dict['index'] = index
        }
        def type = typeelements.join("_")
        dict['type'] = (type ? type : fallbackType)
        return dict
    }
    */

    private GetResponse getFromElastic(index, type, id) {
        GetResponse response  = null
        try {
            log.debug("Before execution")
            response = client.prepareGet(index, type, id).setFields("_source","_timestamp").execute().actionGet()
            log.debug("After execution")
        } catch (Exception e) { 
            log.debug("Failed to get response from server: " + e.getMessage())
        }
        return response
    }

    @Override
    Document get(URI uri) {
        log.debug("Received GET request for $uri")
        GetResponse response = null
        int failcount = 0
        while (response == null) {
            log.debug("Awaiting response ...")
            response = getFromElastic(index, storageType, translateIdentifier(uri))
            if (response == null) {
                log.debug("Retrying server connection ...")
                if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to connect to elasticsearch after $failcount attempts.")
                }
                Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
            } 
        }
        if (response && response.exists()) {
            def ts = (response.field("_timestamp") ? response.field("_timestamp").value : null)
            return deserializeJsonDocument(response.source(), uri, ts) 
        }
        return null
    }

    def Collection<LogEntry> updates(Date since, int start = 0) {
        def srb = client.prepareSearch(index).addField("_timestamp").setTypes(storageType).setFrom(start).setSize(BATCH_SIZE).addSort("_timestamp", org.elasticsearch.search.sort.SortOrder.ASC)
        def query = rangeQuery("_timestamp").gte(since.getTime())
        srb.setQuery(query)
        log.debug("Logquery: " + srb)
        def response = srb.execute().actionGet()
        log.debug("Response: " + response)
        //return null
        def results = new ArrayList<LogEntry>()
        if (response) {
            log.debug "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each { 
                results.add(new LogEntry(translateIndexIdTo(it.id), new Date(it.field("_timestamp").value)))
            }
        }

        return results
    }


    def performQuery(Query q) {
        def srb = client.prepareSearch(index).setTypes(indexType)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setFrom(q.start).setSize(q.n)
        def query = queryString(q.query)
        if (q.fields) {
            q.fields.each {
                query = query.field(it)
            }
        }
        srb.setQuery(query)
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
        log.debug("SearchRequestBuilder: " + srb)
        def response = srb.execute().actionGet()
        log.debug("SearchResponse: " + response)
        return response
    }

    def Map<String, String[]> convertHighlight(Map<String, HighlightField> hfields) {
        def map = new TreeMap<String, String[]>()
        hfields.each {
            map.put(it.value.name, it.value.fragments)
        }
        return map
    }

    @Override
    SearchResult query(Query q) {
        log.debug "Doing query on $q"
        def response = null
        int failcount = 0
        while (!response) {
             response = performQuery(q)
             if (!response) {
                 log.debug("Retrying server connection ...")
                 if (failcount++ > WARN_AFTER_TRIES) {
                    log.warn("Failed to connect to elasticsearch after $failcount attempts.")
                 }
                 Thread.sleep(RETRY_TIMEOUT + failcount > MAX_RETRY_TIMEOUT ? MAX_RETRY_TIMEOUT : RETRY_TIMEOUT + failcount)
             }
        }

        def results = new BasicSearchResult(response.hits.totalHits)

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
        }     
        return results
    }

    Document createDocumentFromHit(hit) {
        return new BasicDocument().withData(hit.source()).withIdentifier(translateIndexIdTo(hit.id))
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(source, uri, timestamp) {
        Gson gson = new Gson()
        Document doc = gson.fromJson(new String(source), BasicDocument.class)
        return doc
    }

    def serializeDocumentToJson(Document doc) {
        Gson gson = new Gson()
        def json = gson.toJson(doc)
        return json
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    // Force one-client-per-whelk
    ElasticSearchClient(String i) {
        this.index = i
        Properties properties = new Properties();
        def is = ElasticSearchClient.class.getClassLoader().getResourceAsStream("whelks-core.properties")
        properties.load(is);
        final String elastichost = properties.getProperty("elastichost");

        log.debug "Connecting to elastichost:9300"
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30)
                .put("cluser.name", "rockpool")
                .build();
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(elastichost, 9300))
        log.debug("... connected")
        init()
    }
} 

@Log
class ElasticSearchNode extends ElasticSearch {

    def ElasticSearchNode() {
        log.debug "Creating elastic node"
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
        // here you can set the node and index settings via API
        settings.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        //
        // start it!
        def node = nBuilder.build().start()
        client = node.client()
        log.debug "Client connected to new ES node"
    }
}

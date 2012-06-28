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
    int MAX_TRIES = 1000
    int RETRY_TIMEOUT = 300

    String defaultType = "record"
    String storageType = "document"

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    def init() {
        log.debug("Creating index ...")
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject(index)
            .startObject("_timestamp")
            .field("enabled", true)
            .field("store", true)
            .endObject()
            .endObject()
            .endObject()
        println "mapping: " + mapping.string()

        client.admin().indices().prepareCreate(index).addMapping(storageType, mapping).execute()
        println "Created ..."
    }

    def boolean add(Document doc, String addType) {
        boolean unfailure = false
        def dict = determineIndexAndType(doc.identifier, addType)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        try {
            IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setTimestamp(""+doc.getTimestamp()).setSource(serializeDocumentToJson(doc, addType)).execute().actionGet()
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
            if (failcount++ > MAX_TRIES) {
                log.error("Failed to store document after $MAX_TRIES attempts")
                break;
            }
            Thread.sleep(RETRY_TIMEOUT + failcount)
        }
    }

    @Override
    void index(Document doc) {
        addLoop(doc, defaultType)
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

    private GetResponse getFromElastic(index, type, id) {
        GetResponse response  = null
        try {
            response = client.prepareGet(index, type, id).setFields("_source","_timestamp").execute().actionGet()
        } catch (Exception e) { 
            log.debug("Failed to get response from server.", e)
        }
        return response
    }

    @Override
    Document get(URI uri) {
        def dict = determineIndexAndType(uri, storageType)
        GetResponse response = null
        GetResponse metaresponse = null
        int failcount = 0
        while (response == null) {
            response = getFromElastic(dict['index'], dict['type'], dict['id'])
            if (response == null) {
                log.debug("Retrying server connection ...")
                if (failcount++ > MAX_TRIES) {
                    log.error("Failed to connect to elasticsearch after $MAX_TRIES attempts.")
                    break
                }
                Thread.sleep(RETRY_TIMEOUT + failcount)
            } 
        }
        if (response && response.exists()) {
            def ts = (response.field("_timestamp") ? response.field("_timestamp").value : null)
            return deserializeJsonDocument(response.source(), uri, ts) 
        }
        return null
    }

    def Iterable<LogEntry> updates(Date since) {
        def srb = client.prepareSearch(index).addField("_timestamp").setTypes(storageType)
        def query = rangeQuery("_timestamp").gte(since.getTime())
        //def query = rangeQuery("fields.001").gte("191502")
        srb.setQuery(query)
        log.debug("Logquery: " + srb)
        def response = srb.execute().actionGet()
        log.debug("Response: " + response)
        //return null
        def results = new ArrayList<LogEntry>()
        if (response) {
            log.debug "Total log hits: ${response.hits.totalHits}"
            response.hits.hits.each { 
                def uri = new URI("/" + it.index + (it.type == this.defaultType || it.type == this.storageType ? "" : "/" + it.type) + "/" + it.id)
                results.add(new LogEntry(uri, new Date(it.field("_timestamp").value)))
            }
        }

        return results
    }

    def performQuery(Query q) {
        def srb = client.prepareSearch(index)
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
                 if (failcount++ > MAX_TRIES) {
                    log.error("Failed to connect to elasticsearch after $MAX_TRIES attempts.")
                     break
                 }
                 Thread.sleep(RETRY_TIMEOUT + failcount)
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
        def u = new URI("/" + hit.index + (hit.type == this.defaultType || hit.type == this.storageType ? "" : "/" + hit.type) + "/" + hit.id)
        return new BasicDocument().withData(hit.source()).withIdentifier(u)
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(source, uri, timestamp) {
        def slurper = new JsonSlurper()
        def slurped = slurper.parseText(new String(source))
        def contentType = "application/json"
        Document doc = null
        if (slurped.data) {
            def data = slurped.data

            def version = slurped.version
            def size = slurped.size
            contentType = slurped.contenttype
            doc = new BasicDocument().withData(data).withIdentifier(uri).withContentType(contentType)
        } else {
            doc = new BasicDocument().withData(source).withIdentifier(uri).withContentType(contentType)
        }
        if (timestamp) {
            doc.setTimestamp(new Date(timestamp))
        }
        return doc
    }

    def serializeDocumentToJson(Document doc, String addMode) {
        if (doc.contentType != "application/json" || addMode == storageType) {
            log.debug("Wrapping document.")
            Gson gson = new Gson()
            def docrepr = [:]
            docrepr['data'] = new String(doc.data)
            docrepr['uri'] = doc.identifier
            docrepr['contenttype'] = doc.contentType
            docrepr['size'] = doc.size
            docrepr['version'] = doc.version
            return gson.toJson(docrepr)
        }
        return doc.data
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

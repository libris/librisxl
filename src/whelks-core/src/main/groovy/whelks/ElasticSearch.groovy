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
class ElasticSearch implements Index, Storage {

    Whelk whelk
    Client client

    boolean enabled = true
    String id = "elasticsearch"
    int MAX_TRIES = 1000
    int RETRY_TIMEOUT = 300

    String defaultType = "record"

    def void setWhelk(Whelk w) { this.whelk = w; init() }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    def init() {
        log.debug("Creating index ...")
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject(this.whelk.prefix)
            .startObject("_timestamp")
            .field("enabled", true)
            .field("store", true)
            .endObject()
            .endObject()
            .endObject()
        println "mapping: " + mapping.string()

        client.admin().indices().prepareCreate(this.whelk.prefix).addMapping(defaultType, mapping).execute()
        println "Created ..."
    }

    def boolean add(Document doc) {
        boolean unfailure = false
        def dict = determineIndexAndType(doc.identifier)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        try {
            IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setTimestamp(""+doc.updateTimestamp().getTimestamp()).setSource(serializeDocumentToJson(doc)).execute().actionGet()
            println "response: " + response
            //response = client.prepareIndex(dict.index, dict.type+":meta", dict.id).setSource(extractMetaData(doc)).execute().actionGet()
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

    byte[] extractMetaData(Document doc) {
        def builder = new groovy.json.JsonBuilder()
        builder {
            "uri"(doc.identifier.toString()) 
            "version"(doc.version) 
            "contenttype"(doc.contentType) 
            "size"(doc.size)
        }
        //println "Metadata: ${builder.toString()}"
        return builder.toString().getBytes()
    }

    @Override
    void index(Document doc) {
        int failcount = 0
        while (!add(doc)) {
            if (failcount++ > MAX_TRIES) {
                log.error("Failed to store document after $MAX_TRIES attempts")
                break;
            }
            Thread.sleep(RETRY_TIMEOUT + failcount)
        }
    }

    /**
     * Since ES can't handle anything but JSON, we need to wrap other types of data in a JSON wrapper before storing.
     */
    def wrapData(byte[] data, URI identifier) {
        if (isJSON(data)) {
            return data
        } else {
            Gson gson = new Gson()
            def docrepr = [:]
            docrepr['data'] = new String(data)
            docrepr['identifier'] = identifier
            docrepr['contenttype'] = "text/plain"
            String json = gson.toJson(docrepr)
            return json.getBytes()
        }
    }

    void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    public void store(Document d) {
        log.debug("Not storing document, because we are also used as Index. Unnecessary to store twice.")
    }

    OutputStream getOutputStreamFor(Document doc) {
        log.debug("Preparing outputstream for document ${doc.identifier}")
            return new ByteArrayOutputStream() {
                void close() throws IOException {
                    doc = doc.withData(toByteArray())
                    ElasticSearch.this.add(doc)
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

    def determineIndexAndType(URI uri) {
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
            dict['index'] = whelk.prefix
        }
        def type = typeelements.join("_")
        dict['type'] = (type ? type : this.defaultType)
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
    Document get(URI uri, raw = false) {
        def dict = determineIndexAndType(uri)
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
            Document d 
            def map = response.sourceAsMap()
            log.debug("Raw mode: ${raw}")
            /*
            map.each {
            log.debug("MAP: " + it.key +":" + it.value)
            }
            */
            if (raw) {
                d = this.whelk.createDocument().withIdentifier(uri).withContentType("application/json").withData(response.source())
            } else {
                d = deserializeJsonDocument(response.source(), uri, getFromElastic(dict['index'], dict['type']+":meta", dict['id']))
            }

            return d
        }
        return null
    }

    def performLogQuery(Date since) {
      def srb = client.prepareSearch(this.whelk.prefix)
      def query = rangeQuery("_timestamp").gte(since)
      //def query = rangeQuery("fields.001").gte("191502")
      srb.setQuery(query)
      log.debug("Logquery: " + srb)
      def response = srb.execute().actionGet()
      log.debug("Response: " + response)
    }

    def performQuery(Query q) {
        def srb = client.prepareSearch(this.whelk.prefix)
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
        def u = new URI("/" + hit.index + (hit.type == this.defaultType ? "" : "/" + hit.type) + "/" + hit.id)
        return this.whelk.createDocument().withData(hit.source()).withIdentifier(u)
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(data, uri, metaresponse) {
        def version, size
        def contentType = "application/json"
        if (metaresponse && metaresponse.exists()) {
            def slurper = new JsonSlurper()
            def slurped = slurper.parseText(new String(data))
            version = slurped.version
            size = slurped.size
            contentType = (slurped.contenttype ? slurped.contenttype : contentType)
        }
        Document doc = this.whelk.createDocument().withData(data).withIdentifier(uri).withContentType(contentType)
        return doc
    }

    def serializeDocumentToJson(Document doc) {
        if (doc.contentType != "application/json") {
            log.debug("Document is not JSON, must wrap it.")
            Gson gson = new Gson()
            def docrepr = [:]
            docrepr['data'] = new String(doc.data)
            docrepr['uri'] = doc.identifier
            docrepr['contenttype'] = "text/plain"
            docrepr['size'] = doc.size
            docrepr['version'] = doc.version
            return gson.toJson(docrepr)
        }
        return doc.data
    }

}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
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

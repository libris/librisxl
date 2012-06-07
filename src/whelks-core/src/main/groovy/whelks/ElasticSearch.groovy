package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.*

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*

import org.json.simple.*
import com.google.gson.*

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

    String defaultType = "record"

    def void setWhelk(Whelk w) { this.whelk = w }

    def void enable() {this.enabled = true}
    def void disable() {this.enabled = false}

    def add(Document doc) {
        log.debug "Indexing document ..."
        def dict = determineIndexAndType(doc.identifier)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        try {
            IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setSource(serializeDocumentToJson(doc)).execute().actionGet()
            log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        } catch (org.elasticsearch.index.mapper.MapperParsingException me) {
            log.error("Failed to index document with id ${doc.identifier}: " + me.getMessage(), me)
        } catch (org.elasticsearch.client.transport.NoNodeAvailableException nnae) {
            log.fatal("Failed to connect to elasticsearch node: " + nnae.getMessage(), nnae)
        }
    }

    @Override
    void index(Document doc) {
        log.debug("Not indexing document, because we are also used as Storage. Unnecessary to store twice.")
        //add(doc.data, doc.identifier)
    }

    void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    @Override
    public void store(Document d) {
        add(d)
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
            dict['index'] = whelk.defaultIndex
        }
        def type = typeelements.join("_")
        dict['type'] = (type ? type : this.defaultType)
        return dict
    }

    private GetResponse getFromElastic(index, type, id) {
        GetResponse response  = null
        try {
            response = client.prepareGet(index, type, id).execute().actionGet()
        } catch (Exception e) { 
            log.warn("Failed to get response from server.", e)
        }
        return response
    }

    @Override
    Document get(URI uri, raw = false) {
        def dict = determineIndexAndType(uri)
        GetResponse response = null
        int failcount = 0
        while (response == null) {
            response = getFromElastic(dict['index'], dict['type'], dict['id'])
            if (response == null) {
                log.warn("Retrying server connection ...")
                if (failcount++ > 20) {
                    break
                }
                Thread.sleep(100)
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
                d = deserializeJsonDocument(response.source())
            }

            return d
        }
        return null
    }

    @Override
    SearchResult query(String query) { //, boolean raw = false) {
        log.debug "Doing query on $query"
        def srb 
        def index = whelk.name
        if (index == null) {
            srb = client.prepareSearch()  
        } else {
            srb = client.prepareSearch(index)  
        }
        SearchResponse response = srb
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(queryString(query))
            //.setFrom(0).setSize(60)
            .setExplain(true)
            .execute()
            .actionGet()
        log.debug "Total hits: ${response.hits.totalHits}"
        def results = new BasicSearchResult()
        response.hits.hits.each {
            results.addHit(deserializeJsonDocument(it.source()))
        }
        /*
        if (raw) {
            return new BasicSearchResult(response.toString(), response.hits.totalHits)
            //return response.toString()
        } else {
            def hits = new StringBuilder()
            response.hits.hits.eachWithIndex() { it, i ->
                if (i > 0) { hits << "," }
                hits << new String(it.source())
            }
            hits = hits.insert(0,"[")
            hits = hits.append("]")
            return new BasicSearchResult(hits.toString(), response.hits.totalHits)
        }
        */
        return results
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(data) {
        def jsonData = (JSONObject)JSONValue.parse(new String(data, 'UTF-8'))
        Document doc = this.whelk.createDocument()
            .withIdentifier(jsonData.get("uri"))
            .withContentType(jsonData.get("contenttype"))
            .withData(jsonData.get("data").toString().getBytes())
        /*
        Gson gson = new Gson()
        Document doc = gson.fromJson(new String(data), BasicDocument.class)
        */
        return doc
    }

    def serializeDocumentToJson(Document doc) {
        //return doc.data
        Gson gson = new Gson()
        if (doc.contentType == "application/json") {
            def jsonString = new StringBuilder("{")
            jsonString << "\"uri\": \"${doc.identifier}\","
            jsonString << "\"version\": \"${doc.version}\","
            jsonString << "\"contenttype\": \"${doc.contentType}\","
            jsonString << "\"size\": ${doc.size},"
            jsonString << "\"data\":" << (isJSON(doc.dataAsString) ? doc.dataAsString : "\"${doc.dataAsString}\"") << "}"
            println "JSON" + jsonString.toString()
            return jsonString.toString().getBytes()
        } else {
            def docrepr = [:]
            docrepr['data'] = new String(doc.data)
            docrepr['uri'] = doc.identifier
            docrepr['contenttype'] = "text/plain"
            docrepr['size'] = doc.size
            docrepr['version'] = doc.version
            String json = gson.toJson(docrepr)
            return json.getBytes()
        }
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        Properties properties = new Properties();
        def is = ElasticSearchClient.class.getClassLoader().getResourceAsStream("whelks-core.properties")
        println "is is " + is
        properties.load(is);
        final String elastichost = properties.getProperty("elastichost");

        log.debug "Connecting to elastichost:9300"
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ping_timeout", 30).build();
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

package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log
import groovy.json.*

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

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*

import com.google.gson.Gson

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

    /**
     * Since ES can't handle anything but JSON, we need to wrap other types of data in a JSON wrapper before storing.
     */
    def wrapData(byte[] data, URI identifier, String contentType) {
        if (contentType.equalsIgnoreCase("application/json")) {
            if (isJSON(data)) {
                return data
            } else {
                throw new WhelkRuntimeException("Badly formed JSON data for document $identifier")
            }
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

    /*
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
    */

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

    @Override
    Document get(URI uri, raw = false) {
        def dict = determineIndexAndType(uri)
        GetResponse response 
        try {
            response = client.prepareGet(dict['index'], dict['type'], dict['id']).execute().actionGet()
        } catch (Exception e) {
            log.error("Exception", e)
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
            if (!raw && map['contenttype'] != "application/json" && map['data']) {
                if (!map['contenttype']) {
                    map['contenttype'] = contentType(map['data'].getBytes())
                }
                d = this.whelk.createDocument().withIdentifier(uri).withContentType(map['contenttype']).withData(map['data'])
            } else {
                d = this.whelk.createDocument().withIdentifier(uri).withContentType("application/json").withData(response.source())
            }

            return d
        }
        return null
    }

    @Override
    SearchResult query(String query, boolean raw = false) {
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
        log.debug("Raw mode: ${raw}")
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
    }

    @Override
    LookupResult lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.")
    }

    Document deserializeJsonDocument(data) {

    }

    def serializeDocumentToJson(Document doc) {
        return doc.data
        def jsonString = new StringBuilder("{")
        jsonString << "\"uri\": \"${doc.identifier}\","
        jsonString << "\"version\": \"${doc.version}\","
        jsonString << "\"contenttype\": \"${doc.contentType}\","
        jsonString << "\"size\": ${doc.size},"
        jsonString << "\"data\":" << doc.dataAsString << "}"
        /*
        def builder = new JsonBuilder()
        def root = builder {
            uri doc.identifier.toString()
            version doc.version
            contenttype doc.contentType
            size doc.size
            /* TODO
            links doc.links.asList()
            keys doc.keys.asList()
            tags doc.tags.asList()
        }
        def slurper = new JsonSlurper()
        root['data'] = slurper.parseText(new String(doc.data, 'UTF-8'))
        println "RESULT"
        print builder.toString()
            */
        return jsonString.toString()
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        log.debug "Connecting to devdb.libris.kb.se:9300"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("devdb.libris.kb.se", 9300))
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

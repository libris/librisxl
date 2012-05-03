package se.kb.libris.conch.component

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

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.node.NodeBuilder.*

import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import com.google.gson.JsonObject

import se.kb.libris.whelks.Document
import se.kb.libris.conch.Whelk
import se.kb.libris.conch.data.MyDocument
import se.kb.libris.conch.component.*

@Log
class ElasticSearch implements Index, Storage {

    Whelk whelk
    Client client

    String defaultType = "record"

    def setWhelk(Whelk w) { this.whelk = w }

    def add(Document d) {
        log.debug "Indexing document ..."
        def dict = determineIndexAndType(d.identifier)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setSource(_wrap_data(d)).execute().actionGet()
        log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        def iresp = [:]
        iresp['id'] = response.id
        iresp['index'] = response.index
        iresp['type'] = response.type
        return iresp
    }

    /**
     * Since ES can't handle anything but JSON, we need to wrap other types of data in a JSON wrapper before storing.
     */
    def _wrap_data(doc) {
        Gson gson = new Gson()
        if (!_is_json(new String(doc.data))) {
            def docrepr = [:]
            docrepr['data'] = new String(doc.data)
            docrepr['identifier'] = doc.identifier
            String json = gson.toJson(docrepr)
            return json.getBytes()
        } else {
            /*
            def jsonmap = [:]
            jsonmap['identifier'] = '"' + doc.identifier.toString() + '"'
            jsonmap['data'] = new String(doc.data)
            return _assemble_json_map(jsonmap)
            */
            return doc.data
        }
    }

    def _assemble_json_map(jsonmap) {
        def jsondata = new StringBuffer("{")
        jsonmap.eachWithIndex() { it, i ->
            if (i > 0) { jsondata << ","}
            jsondata << '"' + it.key + '": ' + it.value
        }
        jsondata << "}"
        return jsondata.toString().getBytes()
    }

    def _is_json(def data) {
        Gson gson = new Gson()
        try {
            gson.fromJson(data, Object.class)
        } catch (JsonSyntaxException jse) {
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
            dict['index'] = whelk.name
        }
        def type = typeelements.join("_")
        dict['type'] = (type ? type : this.defaultType)
        return dict
    }

    def retrieve(URI uri) {
        def dict = determineIndexAndType(uri)
        GetResponse response 
        try {
            response = client.prepareGet(dict['index'], dict['type'], dict['id']).execute().actionGet()
            println "Got response ${response.class.name}"
        } catch (Exception e) {
            log.error("Exception", e)
        }
        if (response.exists()) {
            return new MyDocument(uri).withData(new String(response.sourceAsString()).getBytes())
        }
        return null
    }

    def find(def query, def index, def raw = false) {
        log.debug "Doing query on $query"
        SearchResponse response = client.prepareSearch(index)
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(queryString(query))
        //.setFrom(0).setSize(60)
        .setExplain(true)
        .execute()
        .actionGet()
        log.debug "Total hits: ${response.hits.totalHits}"
        def hits = new StringBuilder()
        response.hits.hits.eachWithIndex() { it, i ->
            if (i > 0) { hits << "," }
            hits << new String(it.source())
        }
        if (response.hits.totalHits() > 1) {
            hits = hits.insert(0,"[")
            hits = hits.append("]")
        }
        
        return hits.toString()
        //return response.toString()
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        log.debug "Connecting to devdb.libris.kb.se:9300"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("193.10.75.219", 9300))
        //client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200))
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

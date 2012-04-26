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

import se.kb.libris.whelks.Document
import se.kb.libris.conch.Whelk
import se.kb.libris.conch.data.MyDocument
import se.kb.libris.conch.component.*

@Log
class ElasticSearch implements Index {

    Whelk whelk
    Client client

    String defaultType = "record"

    def setWhelk(Whelk w) { this.whelk = w }

    def add(Document d) {
        log.debug "Indexing document ..."
        def dict = determineIndexAndType(d.identifier)
        log.debug "Should use index ${dict.index}, type ${dict.type} and id ${dict.id}"
        IndexResponse response = client.prepareIndex(dict.index, dict.type, dict.id).setSource(d.data).execute().actionGet()
        log.debug "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        def iresp = [:]
        iresp['id'] = response.id
        iresp['index'] = response.index
        iresp['type'] = response.type
        return iresp
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
        GetResponse response = client.prepareGet(dict['index'], dict['type'], dict['id']).execute().actionGet()
        if (response.exists()) {
            return new MyDocument(uri).withData(new String(response.sourceAsString()).getBytes())
        }
        return null
    }

    def find(def query) {
        log.debug "Doing query on $query"
        SearchResponse response = client.prepareSearch(this.whelk.name)
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(queryString(query))
        //.setFrom(0).setSize(60)
        .setExplain(true)
        .execute()
        .actionGet()
        log.debug "Total hits: ${response.hits.totalHits}"
        return response.toString()
    }
}

@Log
class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        log.debug "Connecting to localhost:9200"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200))
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

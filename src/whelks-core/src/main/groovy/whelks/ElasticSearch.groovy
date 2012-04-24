package se.kb.libris.conch.component

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
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
import se.kb.libris.conch.component.*

class ElasticSearch implements Index {

    Whelk whelk
    Client client

    def setWhelk(Whelk w) { this.whelk = w }

    def add(Document d) {
        index(d, d.index, d.type)
    }

    def retrieve(URI uri) {

    }

    def find(def query) {
        println "Doing query on $query"
        SearchResponse response = client.prepareSearch(this.whelk.name)
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(queryString(query))
        //.setFrom(0).setSize(60)
        .setExplain(true)
        .execute()
        .actionGet()
        println "Response: ${response.hits.totalHits}"
        return response.toString()
    }

    def index(Document d, def indexName, def type) {
        println "Indexing document ..."
        IndexResponse response = client.prepareIndex(indexName, type, d.identifier.toString()).setSource(d.data).execute().actionGet()
        println "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}" 
        def iresp = []
        iresp['id'] = response.id
        iresp['index'] = response.index
        iresp['type'] = response.type
        return iresp
    }
}

class ElasticSearchClient extends ElasticSearch {

    def ElasticSearchClient() {
        println "Connecting to localhost:9200"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200))
    }
} 


class ElasticSearchNode extends ElasticSearch {

    def ElasticSearchNode() {
        println "Creating elastic node"
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
        // here you can set the node and index settings via API
        settings.build()
        NodeBuilder nBuilder = nodeBuilder().settings(settings)
        //
        // start it!
        def node = nBuilder.build().start()
        client = node.client()
        println "Client connected to new ES node"
    }
}

package se.kb.libris.conch

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder

import static org.elasticsearch.node.NodeBuilder.*

import se.kb.libris.whelks.Document
import se.kb.libris.conch.component.*

class ElasticSearchIndex implements Index {
    Client client

    def _createIndex(indexName) {
        try {
            client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()
        } catch(Exception ex) {
            println "Index already exists " + ex.getMessage()
        }
    }

    def add(Document d) {
        index(d, d.index, d.type)
    }

    def find(def query) {}

    def index(Document d, def indexName, def type) {
        //_createIndex(indexName)
        println "Indexing document ..."
        IndexResponse response = client.prepareIndex(indexName, type, d.identifier.toString()).setSource(d.data).execute().actionGet()
        println "Indexed document with id: ${response.id}, in index ${response.index} with type ${response.type}"
    }
}

class ElasticSearchClientIndex extends ElasticSearchIndex {

    def ElasticSearchClientIndex() {
        println "Connecting to localhost:9200"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200))
    }
} 


class ElasticSearchNodeIndex extends ElasticSearchIndex {

    def ElasticSearchNodeIndex() {
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

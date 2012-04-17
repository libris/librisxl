package se.kb.libris.conch

import se.kb.libris.whelks.Document
import static org.elasticsearch.node.NodeBuilder.*
import org.elasticsearch.node.Node
import org.elasticsearch.client.Client
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.get.GetResponse

class ElasticSearchIndex extends BasicIndex {

    Node node
    Client client

    ElasticSearchIndex() {
        println "Initializing index"
        node = nodeBuilder().clusterName('myCluster').node();
        client = node.client();
    }

    def index(Document d, def indexName, def type) {
        IndexResponse response = client.prepareIndex(indexName, type, d.identifier.toString()).setSource(jsonify(d)).execute().actionGet()
        println "Indexresponse: $response.id, $response.index, $response.type"
    }

    def find(def indexName, def type, def identifier) {
        def response = client.prepareGet(indexName, type, identifier).execute().actionGet()
        return response.sourceAsString()
    }

    def jsonify(Document d) {
        return d.data
    }

    public void finalize() {
        node.close();
    }
} 

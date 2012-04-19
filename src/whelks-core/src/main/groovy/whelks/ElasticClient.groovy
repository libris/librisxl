package se.kb.libris.conch

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

import se.kb.libris.whelks.Document

class ElasticSearchClientIndex implements Index {

    Client client

    def ElasticSearchClientIndex() {
        println "Connecting to localhost:9300"
        client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300))
    }

    def index(Document d, def indexName, def type) {
        println "Indexing!  (not really)"
    }
} 


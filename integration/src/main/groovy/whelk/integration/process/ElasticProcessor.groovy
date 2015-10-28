package whelk.integration.process

import groovy.util.logging.Log
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.delete.DeleteRequest

import whelk.JsonLd
import whelk.filter.JsonLdLinkExpander
import whelk.component.ElasticSearch

@Log
class ElasticProcessor implements org.apache.camel.Processor {
    private static final String DEFAULT_INDEX_NAME = "libris"
    private static final String DEFAULT_INDEX_TYPE = "bib"
    private static final String DEFAULT_OPERATION = "INDEX" //default elasticsearch operation (operations: INDEX, BULK_INDEX or DELETE)
    private String elasticCluster, elasticHost
    private int elasticPort
    private JsonLdLinkExpander jsonLdLinkExpander //TODO: needs config param "nodesToExpand" (src/main/resources/plugins.json)


    public ElasticProcessor(String elasticCluster, String elasticHost, String elasticPort, jsonLdLinkExpander) {
        this.elasticCluster = elasticCluster
        this.elasticHost = elasticHost
        this.elasticPort = elasticPort
        this.jsonLdLinkExpander = jsonLdLinkExpander
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()

        String identifier = message.getHeader("document:identifier")
        String dataset = message.getHeader("document:dataset")
        String indexName = message.getHeader("whelk:index", DEFAULT_INDEX_NAME)
        message.setHeader("whelk:index", indexName)

        String indexType = message.getHeader("document:dataset", DEFAULT_INDEX_TYPE)
        message.setHeader("whelk:type", indexType)

        String elasticId = toElasticId(identifier)
        String operation = message.getHeader("whelk:operation", DEFAULT_OPERATION)

        message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=${operation}&indexType=${indexType}&indexName=${indexName}")

        log.trace("Processing $operation MQ message for ${indexName}. ID: $identifier (encoded: $elasticId)")

        if (operation == "DELETE") {
            log.debug("Setting message body to delete request for $elasticId in preparation for REMOVE operation.")
            log.debug("Setting elasticDestination: ${message.getHeader('elasticDestination')}")
            DeleteRequest deleteRequest = new DeleteRequest(indexName, indexType, elasticId)
            message.setBody(deleteRequest)
        } else {
            log.trace("Setting elasticDestination: ${message.getHeader('elasticDestination')}")
            def dataMap = JsonLd.frame(identifier, message.getBody(Map.class))

            dataMap = jsonLDLinkExpander.doFilter(dataMap, dataset)

            def idxReq = new IndexRequest(indexName, indexType, elasticId).source(dataMap)
            message.setBody(idxReq)
        }
        exchange.setOut(message)
    }
}

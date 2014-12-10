package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.component.*
import whelk.plugin.*

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor

import org.codehaus.jackson.map.ObjectMapper

@Log
class ElasticTypeRouteProcessor implements Processor {

    static final ObjectMapper mapper = new ObjectMapper()

    ElasticShapeComputer shapeComputer
    String elasticHost, elasticCluster
    int elasticPort

    //ElasticTypeRouteProcessor(String elasticHost, String elasticCluster, int elasticPort, List<String> availableTypes, ElasticShapeComputer esc) {
    ElasticTypeRouteProcessor(ElasticShapeComputer index) {
        this.shapeComputer = index
        this.elasticHost = index.elastichost
        this.elasticPort = index.elasticport
        this.elasticCluster = index.elasticcluster
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        String identifier = message.getHeader("entry:identifier")
        String indexName = message.getHeader("whelk:index", shapeComputer.whelk.id)
        message.setHeader("whelk:index", indexName)
        String indexType = shapeComputer.calculateTypeFromIdentifier(identifier)
        message.setHeader("whelk:type", indexType)
        String indexId = shapeComputer.toElasticId(identifier)
        message.setHeader("whelk:id", indexId)
        String operation = message.getHeader("whelk:operation")
        if (operation == Whelk.ADD_OPERATION) {
            operation = "INDEX"
        }
        if (operation == Whelk.BULK_ADD_OPERATION) {
            operation = "BULK_INDEX"
        }
        log.debug("Processing $operation MQ message for ${indexName}. ID: $identifier (encoded: $indexId)")

            message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=${operation}&indexName=${indexName}&indexType=${indexType}")
            if (operation == Whelk.REMOVE_OPERATION) {
                log.debug(">>> Setting message body to $indexId in preparation for REMOVE operation.")
                message.setBody(indexId)
            } else {
                def dataMap = mapper.readValue(new String(message.getBody(), "UTF-8"), Map)
                dataMap.put("encodedId", indexId)
                message.setBody(dataMap)
            }
        exchange.setOut(message)
    }
}

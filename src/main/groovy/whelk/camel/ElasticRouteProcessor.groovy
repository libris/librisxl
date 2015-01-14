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
class ElasticRouteProcessor extends BasicPlugin implements Processor {

    ElasticShapeComputer shapeComputer
    String elasticHost, elasticCluster
    int elasticPort

    private ElasticRouteProcessor() {}
    private static final ElasticRouteProcessor erp = new ElasticRouteProcessor()
    public static ElasticRouteProcessor getInstance() { return erp }

    void bootstrap(String whelkName) {
        this.shapeComputer = getPlugin("index")
        this.elasticHost = shapeComputer.getElasticHost()
        this.elasticCluster = shapeComputer.getElasticCluster()
        this.elasticPort = shapeComputer.getElasticPort()
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        String identifier = message.getHeader("document:identifier")
        String indexName = message.getHeader("whelk:index", shapeComputer.whelk.id)
        message.setHeader("whelk:index", indexName)
        String indexType = shapeComputer.calculateTypeFromIdentifier(identifier)
        message.setHeader("whelk:type", indexType)
        String elasticId = shapeComputer.toElasticId(identifier)
        message.setHeader("elastic:id", elasticId)
        String operation = message.getHeader("whelk:operation")
        if (operation == Whelk.ADD_OPERATION) {
            operation = "INDEX"
        }
        if (operation == Whelk.BULK_ADD_OPERATION) {
            operation = "BULK_INDEX"
        }
        log.debug("Processing $operation MQ message for ${indexName}. ID: $identifier (encoded: $elasticId)")

            message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=${operation}&indexName=${indexName}&indexType=${indexType}")
            if (operation == Whelk.REMOVE_OPERATION) {
                log.debug(">>> Setting message body to $elasticId in preparation for REMOVE operation.")
                message.setBody(elasticId)
            } else {
                def dataMap = mapper.readValue(new String(message.getBody(), "UTF-8"), Map)
                dataMap.put("encodedId", elasticId)
                message.setBody(dataMap)
            }
        exchange.setOut(message)
    }
}

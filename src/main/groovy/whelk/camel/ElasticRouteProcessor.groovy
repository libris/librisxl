package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.component.*
import whelk.plugin.*

import org.apache.camel.*

@Log
class ElasticRouteProcessor extends BasicPlugin implements Processor {

    ShapeComputer shapeComputer
    String elasticHost, elasticCluster
    int elasticPort

    private ElasticRouteProcessor() {}
    private static final ElasticRouteProcessor erp = new ElasticRouteProcessor()
    public static ElasticRouteProcessor getInstance() { return erp }

    List<Filter> filters

    void bootstrap() {
        this.shapeComputer = getPlugin("index") ?: this.shapeComputer
        assert shapeComputer
        this.elasticHost = shapeComputer.getElasticHost()
        this.elasticCluster = shapeComputer.getElasticCluster()
        this.elasticPort = shapeComputer.getElasticPort()
        filters = plugins.findAll { it instanceof Filter }
        log.info("${this.id} is bootstrapped.")
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        String identifier = message.getHeader("document:identifier")
        String dataset = message.getHeader("document:identifier")
        String indexName = message.getHeader("whelk:index", shapeComputer.getIndexName())
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
            try {
                def dataMap = message.getBody(Map.class)
                for (filter in filters) {
                    log.trace("Applying filter $filter")
                    dataMap = filter.doFilter(dataMap, dataset)
                }
                dataMap.put("encodedId", elasticId)
                message.setBody(dataMap)
            } catch (TypeConversionException tce) {
                log.info("Message body is not json, sending message to stub.")
                message.setHeader("stub:discard")
            }
        }
        exchange.setOut(message)
    }

    boolean isJsonMessage(header) {
        def metaentry = mapper.readValue(header("document:metaentry") as String, Map)
        def ctype = metaentry.entry.contentType
        ctype ==~ /application\/(\w+\+)*json/ || ctype ==~ /application\/x-(\w+)-json/
    }
}

package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.component.*
import whelk.plugin.*

import org.apache.camel.*

@Log
class XinfoRouteProcessor extends BasicPlugin implements Processor {

    Index index
    String elasticHost, elasticCluster
    int elasticPort

    private XinfoRouteProcessor() {}
    private static final XinfoRouteProcessor erp = new XinfoRouteProcessor()
    public static XinfoRouteProcessor getInstance() { return erp }

    List<Filter> filters

    void bootstrap() {
        this.index = getPlugin("index") ?: this.index
        assert index
        this.elasticHost = index.getElasticHost()
        this.elasticCluster = index.getElasticCluster()
        this.elasticPort = index.getElasticPort()
        filters = plugins.findAll { it instanceof Filter }
        log.info("${this.id} is bootstrapped.")
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        String identifier = message.getHeader("document:identifier")
        String dataset = message.getHeader("document:dataset")
        String indexName = message.getHeader("whelk:index", index.getIndexName())
        message.setHeader("whelk:index", indexName)
        String indexType = (message.getHeader("document:dataset") ?: index.calculateTypeFromIdentifier(identifier))
        message.setHeader("whelk:type", indexType)
        String elasticId = index.toElasticId(identifier)
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
        log.debug("Setting elasticDestination: ${message.getHeader('elasticDestination')}")
        if (operation == Whelk.REMOVE_OPERATION) {
            log.debug(">>> Setting message body to $elasticId in preparation for REMOVE operation.")
            message.setBody(elasticId)
        } else {
            if (isJsonMessage(message.getHeader("document:metaentry") as String)) {
                def dataMap = message.getBody(Map.class)
                for (filter in filters) {
                    log.trace("Applying filter ${filter.id} on ${identifier} for dataset $dataset")
                    dataMap = filter.doFilter(dataMap, dataset)
                }
                dataMap.put("encodedId", elasticId)
                message.setBody(dataMap)
            } else {
                log.debug("Message body is not json, sending message to stub.")
                message.setHeader("elasticDestination", "stub:discard")
            }
        }
        exchange.setOut(message)
    }

    boolean isJsonMessage(String json) {
        def metaentry = mapper.readValue(json, Map)
        def ctype = metaentry.entry.contentType
        ctype ==~ /application\/(\w+\+)*json/ || ctype ==~ /application\/x-(\w+)-json/
    }
}

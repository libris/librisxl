package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.plugin.*
import whelk.component.*
import whelk.*

import org.apache.camel.processor.UnmarshalProcessor
import org.apache.camel.spi.DataFormat
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.jackson.JacksonDataFormat

import org.codehaus.jackson.map.ObjectMapper

@Log
class FormatConverterProcessor extends BasicPlugin implements Processor,WhelkAware {

    // Maybe rename to DocumentConverterProcessor

    FormatConverter converter
    Filter expander

    static final ObjectMapper mapper = new ObjectMapper()
    String whelkName
    Whelk whelk

    void bootstrap(String whelkName) {
        this.whelkName = whelkName
        this.converter = plugins.find { it instanceof FormatConverter  }
        this.expander = plugins.find { it instanceof Filter }
        log.info("Calling bootstrap for ${this.id}. converter: $converter expander: $expander plugins: $plugins")
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        log.debug("Received message to ${this.id}.")
        log.debug("Message type: ${message.getHeader('whelk:operation')}")
        if (message.getHeader("whelk:operation") == Whelk.REMOVE_OPERATION) {
            message.setHeader("entry:identifier", message.body)
        } else {
            log.debug("converter: $converter expander: $expander")
            log.debug("all plugins: $plugins")
            Document doc = whelk.get(message.getBody())
            log.debug("Loaded document ${doc?.identifier}")
            if (doc && (converter || expander)) {
                log.debug("Running converter/expander.")
                if (converter) {
                    doc = converter.convert(doc)
                }
                if (expander) {
                    doc = expander.filter(doc)
                }
            }
            if (doc) {
                if (doc.isJson()) {
                    message.setBody(doc.dataAsMap)
                } else {
                    message.setBody(doc.data)
                }
                doc.entry.each { key, value ->
                    message.setHeader("entry:$key", value)
                }
            }
            exchange.setOut(message)
        }
    }
}

@Log
class ElasticTypeRouteProcessor implements Processor {

    List types
    ElasticShapeComputer shapeComputer
    String elasticHost, elasticCluster
    int elasticPort

    //ElasticTypeRouteProcessor(String elasticHost, String elasticCluster, int elasticPort, List<String> availableTypes, ElasticShapeComputer esc) {
    ElasticTypeRouteProcessor(Index index) {
        this.types = index.availableTypes
        this.shapeComputer = index.shapeComputer
        this.elasticHost = index.elastichost
        this.elasticPort = index.elasticport
        this.elasticCluster = index.elasticcluster
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        def dataset = message.getHeader("entry:dataset")
        String identifier = message.getHeader("entry:identifier")
        String indexName = message.getHeader("whelk:index", shapeComputer.whelkName)
        message.setHeader("whelk:index", indexName)
        String indexType = shapeComputer.calculateTypeFromIdentifier(identifier)
        message.setHeader("whelk:type", indexType)
        String indexId = shapeComputer.toElasticId(identifier)
        message.setHeader("whelk:id", indexId)
        String operation = message.getHeader("whelk:operation") ?: "BULK_INDEX"
        if (operation == Whelk.ADD_OPERATION) {
            operation = "BULK_INDEX"
        }
        log.debug("Processing $operation MQ message for ${indexName}. ID: $identifier (encoded: $indexId)")

            message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=${operation}&indexName=${indexName}&indexType=${indexType}")
            if (operation == Whelk.REMOVE_OPERATION) {
                log.info(">>> Setting message body to $indexId")
                message.setBody(indexId)
            } else {
                message.getBody(Map.class).put("elastic_id", indexId)
            }
        exchange.setOut(message)
    }
}

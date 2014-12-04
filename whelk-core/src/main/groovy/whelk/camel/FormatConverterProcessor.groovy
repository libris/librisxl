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
            def body = message.getBody()
            Document doc
            if (body instanceof String) {
                doc = whelk.get(message.getBody())
                log.debug("Loaded document ${doc?.identifier}")
            } else {
                log.debug("Setting document data with type ${body.getClass().getName()}")
                doc = whelk.createDocument(message.getHeader("entry:contentType")).withData(body)
                message.headers.each { key, value ->
                    if (key.startsWith("entry:")) {
                        log.debug("Setting entry $key = $value")
                        doc.entry.put(key.substring(6), value)
                    }
                    if (key.startsWith("meta:")) {
                        log.debug("Setting meta $key = $value")
                        doc.meta.put(key.substring(5), value)
                    }
                }
            }
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
                log.debug("Resetting document ${doc.identifier} in message.")
                    /*
                if (doc.isJson()) {
                    message.setBody(doc.dataAsMap)
                } else {
                */
                    message.setBody(doc.data)
                //}
                doc.entry.each { key, value ->
                    message.setHeader("entry:$key", value)
                }
            }
            exchange.setOut(message)
        }
    }
}

@Log
class APIXProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        String identifier = message.getHeader("entry:identifier")
        log.info("processing $identifier for APIX")
        String dataset = message.getHeader("entry:dataset")

        message.setHeader(Exchange.HTTP_PATH, identifier)
        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")

        exchange.setOut(message)
    }
}

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

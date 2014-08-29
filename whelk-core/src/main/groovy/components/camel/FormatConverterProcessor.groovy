package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.component.*

import org.apache.camel.processor.UnmarshalProcessor
import org.apache.camel.spi.DataFormat
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.jackson.JacksonDataFormat

import org.codehaus.jackson.map.ObjectMapper

@Log
class FormatConverterProcessor extends BasicPlugin implements Processor {

    FormatConverter converter
    LinkExpander expander

    static final ObjectMapper mapper = new ObjectMapper()
    String whelkName

    void bootstrap(String whelkName) {
        this.whelkName = whelkName
        this.converter = plugins.find { it instanceof FormatConverter }
        this.expander = plugins.find { it instanceof LinkExpander }
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        if (converter || expander) {
            def data = message.getBody()
            def entry = [:]
            message.headers.each { key, value ->
                if (key.startsWith("entry:")) {
                    entry.put(key.substring(6), value)
                }
            }
            Document doc = new Document().withData(data).withEntry(entry)
            if (converter) {
                doc = converter.convert(doc)
            }
            if (expander) {
                doc = expander.expand(doc)
            }
            message.setBody(doc.dataAsMap)
        }
        exchange.setOut(message)
    }
}

@Log
class ElasticTypeRouteProcessor implements Processor {

    List types
    ElasticShapeComputer shapeComputer
    String elasticHost
    int elasticPort

    ElasticTypeRouteProcessor(String elasticHost, int elasticPort, List<String> availableTypes, ElasticShapeComputer esc) {
        this.types = availableTypes
        this.shapeComputer = esc
        this.elasticHost = elasticHost
        this.elasticPort = elasticPort
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        def dataset = message.getHeader("entry:dataset")
        def operation = message.getHeader("operation", "ADD")
        String identifier = message.getHeader("entry:identifier")
        String indexName = message.getHeader("extra:index", shapeComputer.whelkName)
        String indexType = shapeComputer.calculateShape(identifier)
        String indexId = shapeComputer.translateIdentifier(new URI(identifier))
        String elasticCluster = System.getProperty("elastic.cluster", BasicElasticComponent.DEFAULT_CLUSTER)
        //if (operation == "ADD") {
        if (dataset && types.contains(dataset)) {
            message.setHeader("typeQDestination", "direct:$dataset")
        } else {
            message.setHeader("typeQDestination", "direct:unknown")
        }

        message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=INDEX&indexName=${indexName}&indexType=${indexType}")
        message.getBody(Map.class).put("elastic_id", indexId)
            /*
        } else if (operation == "DELETE") {
            message.setHeader("typeQDestination", "direct:indexDelete")
            message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${elasticHost}&port=${elasticPort}&operation=DELETE&indexName=${indexName}&indexType=${indexType}")
        } else {
            log.warn("Unknown operation: $operation")
        }
        */
        exchange.setOut(message)
    }
}

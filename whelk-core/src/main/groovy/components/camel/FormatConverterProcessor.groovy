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
    ElasticShapeComputer shapeComputer

    static final ObjectMapper mapper = new ObjectMapper()
    String whelkName

    void bootstrap(String whelkName) {
        this.whelkName = whelkName
        this.converter = plugins.find { it instanceof FormatConverter }
        this.expander = plugins.find { it instanceof LinkExpander }
        this.shapeComputer = plugins.find { it instanceof ElasticShapeComputer }
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        def data = message.getBody()
        def entry = [:]
        message.headers.each { key, value ->
            if (key.startsWith("entry:")) {
                entry.put(key.substring(6), value)
            }
        }
        Document doc = new Document().withData(data).withEntry(entry)

        if (converter || expander) {
            if (converter) {
                doc = converter.convert(doc)
            }
            if (expander) {
                doc = expander.expand(doc)
            }
            data = doc.dataAsMap
        }
        if (data instanceof Map) {
            String indexType = shapeComputer.calculateShape(doc.identifier)
            String elasticCluster = System.getProperty("elastic.cluster", BasicElasticComponent.DEFAULT_CLUSTER)

            message.setHeader("elasticDestination", "elasticsearch://${elasticCluster}?ip=${global.ELASTIC_HOST}&port=${global.ELASTIC_PORT}&operation=INDEX&indexName=${whelkName}&indexType=${indexType}")

            data["elastic_id"] = shapeComputer.translateIdentifier(new URI(doc.identifier))
            message.setBody(mapper.writeValueAsString(data))
            if (message.getHeader("elasticDestination") == null) {
                log.warn("${doc.identifier} has no destionation.")
            }
            exchange.setOut(message)
        } else {
            log.warn("Message body for ${doc.identifier} not json. Will not be able to index.")
        }
    }
}

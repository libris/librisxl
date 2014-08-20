package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.plugin.FormatConverter

import org.apache.camel.processor.UnmarshalProcessor
import org.apache.camel.spi.DataFormat
import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.jackson.JacksonDataFormat

import org.codehaus.jackson.map.ObjectMapper

@Log
class FormatConverterProcessor implements Processor {

    FormatConverter converter
    public static final ObjectMapper mapper = new ObjectMapper()

    Whelk whelk

    FormatConverterProcessor(Whelk w) {
        this.whelk = w
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        try {
            log.info("Start process method")
            Message message = exchange.getIn()
            String identifier = message.getBody()
            Document document = whelk.get(new URI(identifier))
            log.info("Received document with identifier: ${document.identifier}")
            if (converter != null) {
                document = converter.convert(document)
            }
            def docMap = document.dataAsMap
            def idelements = new URI(identifier).path.split("/") as List
            idelements.remove(0)
            docMap["elastic_id"] = idelements.join("::")
            message.setBody(mapper.writeValueAsString(docMap))
            log.info("Setting message: $message")
            exchange.setIn(message)
        } catch (Exception e) {
            log.error("Exception", e)
            throw e
        }
    }
}

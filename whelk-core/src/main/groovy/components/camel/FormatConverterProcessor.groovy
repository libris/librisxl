package se.kb.libris.whelks.camel

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.FormatConverter
import se.kb.libris.whelks.plugin.LinkExpander

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
    LinkExpander expander

    public static final ObjectMapper mapper = new ObjectMapper()

    FormatConverterProcessor(FormatConverter c, LinkExpander e) {
        this.converter = c
        this.expander = e
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        try {
            Message message = exchange.getIn()
            def data = message.getBody()
            if (converter || expander) {
                def entry = [:]
                message.headers.each { key, value ->
                    entry[(key)] = value
                }
                Document doc = new Document().withData(data).withEntry(entry)
                if (converter) {
                    doc = converter.convert(doc)
                }
                if (expander) {
                    doc = expander.expand(doc)
                }
                data = doc.dataAsMap
            }
            String identifier = message.getHeader("identifier")
            def idelements = new URI(identifier).path.split("/") as List
            idelements.remove(0)
            data["elastic_id"] = idelements.join("::")
            message.setBody(mapper.writeValueAsString(data))
            exchange.setIn(message)
        } catch (Exception e) {
            log.error("Exception", e)
            throw e
        }
    }
}

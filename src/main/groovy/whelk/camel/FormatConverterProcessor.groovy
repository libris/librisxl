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


    void bootstrap() {
        this.converter = plugins.find { it instanceof FormatConverter  }
        this.expander = plugins.find { it instanceof Filter }
        log.trace("Calling bootstrap for ${this.id}. converter: $converter expander: $expander plugins: $plugins")
    }


    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        log.debug("Received message to ${this.id}.")
        log.debug("Message type: ${message.getHeader('whelk:operation')}")
        log.debug("Dataset: ${message.getHeader('document:dataset')}")
        if (message.getHeader("whelk:operation") != Whelk.REMOVE_OPERATION) {
            def doc = createDocument(message)
            doc = runConverters(doc)
            prepareMessage(doc, message)
            exchange.setOut(message)
        }
    }
}

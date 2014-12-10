package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.Document

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor


@Log
class APIXProcessor extends FormatConverterProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        Document doc = createAndPrepareDocumentFromMessage(message)
        String identifier = message.getHeader("entry:identifier")
        log.debug("processing $identifier for APIX")
        String dataset = message.getHeader("entry:dataset")

        log.info("Received operation: " + message.getHeader("whelk:operation"))
        message.setHeader(Exchange.HTTP_PATH, identifier)

        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")

        exchange.setOut(message)
    }
}


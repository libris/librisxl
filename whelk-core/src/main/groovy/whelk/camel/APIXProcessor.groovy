package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods


@Log
class APIXProcessor extends FormatConverterProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()

        String operation = message.getHeader("whelk:operation")

        log.debug("processing ${message.getHeader('entry:identifier')} for APIX")
        log.info("Received operation: " + operation)
        log.info("dataset: ${message.getHeader('entry:dataset')}")


        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")
        if (operation == Whelk.REMOVE_OPERATION) {
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.DELETE)
            message.setHeader(Exchange.HTTP_PATH, message.getHeader("entry:identifier"))
        } else {
            def doc = createAndPrepareDocumentFromMessage(message) 
            message.setHeader(Exchange.HTTP_PATH, getVoyagerUri(doc))
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }

        exchange.setOut(message)
    }


    String getVoyagerUri(Document doc) {
        doc.identifier
    }
}


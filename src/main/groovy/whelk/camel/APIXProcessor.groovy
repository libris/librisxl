package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods


@Log
class APIXProcessor extends FormatConverterProcessor implements Processor {

    String apixPathPrefix

    APIXProcessor(String prefix) {
        StringBuilder pathBuilder = new StringBuilder(prefix)
        while (pathBuilder[-1] == '/') {
            pathBuilder.deleteCharAt(pathBuilder.length()-1)
        }
        this.apixPathPrefix = pathBuilder.toString()
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()

        String operation = message.getHeader("whelk:operation")

        log.debug("processing ${message.getHeader('entry:identifier')} for APIX")
        log.debug("Received operation: " + operation)
        log.debug("dataset: ${message.getHeader('entry:dataset')}")
        boolean messagePrepared = false

        if (message.getHeader("CamelHttpPath")) {
            log.debug("Message is already prepped ... Forego all treatment.")
            message.setHeader(Exchange.HTTP_PATH, message.getHeader("CamelHttpPath"))
            messagePrepared = true
        }

        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")
        if (operation == Whelk.REMOVE_OPERATION) {
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.DELETE)
            if (!messagePrepared) {
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + message.getHeader("entry:identifier"))
            }
        } else {
            if (!messagePrepared) {
                def doc = createDocument(message)

                String voyagerUri = getVoyagerUri(doc) ?: "/" + message.getHeader("entry:dataset") +"/new"
                doc = runConverters(doc)
                prepareMessage(doc, message)

                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }

        exchange.setOut(message)
    }


    String getVoyagerUri(Document doc) {
        if (doc.identifier ==~ /\/(auth|bib|hold)\/\d+/) {
            return doc.identifier
        }
        String controlNumber = doc.getDataAsMap().get("controlNumber")
        if (controlNumber) {
            return "/"+doc.dataset+"/"+controlNumber
        }
        return null
    }
}

@Log
class APIXResponseProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        Message message = exchange.getIn()
        if (log.isTraceEnabled()) {
            message.getHeaders().each { key, value -> 
                log.trace("APIX response message header: $key = $value")
            }
        }
        log.info("APIX reponse code: ${message.getHeader('CamelHttpResponseCode')} for ${message.getHeader('CamelHttpMethod')} ${message.getHeader('CamelHttpPath')}")
        if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && message.getHeader("CamelHttpResponseCode") == 200) {
            String xmlBody = message.getBody(String.class)
            def xmlresponse = new XmlSlurper(false,false).parseText(xmlBody)
            if (xmlresponse.@status == "ERROR") {
                log.error("APIX responded with error code ${xmlresponse.@error_code} (${xmlresponse.@error_message}) when calling ${message.getHeader('CamelHttpPath')} for document ${message.getHeader('entry:identifier')}")
            } else {
                log.info("Received XML response from APIX: $xmlBody")
            }
        }
    }
}

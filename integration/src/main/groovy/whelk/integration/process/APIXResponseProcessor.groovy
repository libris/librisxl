package whelk.integration.process

import groovy.util.logging.Slf4j as Log

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import whelk.Document

@Log
class APIXResponseProcessor implements Processor {

    //@Override
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
                log.error("APIX responded with error code ${xmlresponse.@error_code} (${xmlresponse.@error_message}) when calling ${message.getHeader('CamelHttpPath')} for document ${message.getHeader('document:identifier')}")
                log.debug("APIX response: $xmlBody")
                def errorDetails = [:]
                //TODO: storage.load(identifier)
                Document failedDocument = whelk.get(message.getHeader("document:identifier"))
                //TODO: apply filters, if filter.valid(doc)
                def docMeta = failedDocument.meta
                docMeta['apixError'] = true
                docMeta['apixErrorCode'] = xmlresponse.@error_code as String
                docMeta['apixErrorMessage'] = xmlresponse.@error_message as String
                docMeta['apixRequestPath'] = message.getHeader('CamelHttpPath').toString()
                failedDocument.withMeta(docMeta)
                whelk.add(failedDocument, true)
                if (xmlresponse.@error_message.toString().endsWith(" error = 203") && xmlresponse.@error_code.toString() == "2") {
                    message.setHeader("retry", true)
                    log.info("Setting retry with next: ${message.getHeader('JMSDetination')}")
                    message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))
                } else {
                    log.info("Error ${xmlresponse.@error_code} (${xmlresponse.@error_message}) is not deemed retryable.")
                }
            } else {
                log.info("Received XML response from APIX: $xmlBody")
            }
        } else if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && (message.getHeader("CamelHttpResponseCode") == 201 || message.getHeader("CamelHttpResponseCode") == 303)) {
            log.debug("Document created in APIX. Try to harvest the identifier ...")
            try {
                String recordNumber = message.getHeader("Location").split("/")[-1]
                String dataset = message.getHeader("Location").split("/")[-2]
                //TODO: storage.load(identifier)
                Document doc = whelk.get(message.getHeader("document:identifier"))
                //TODO: apply filters, if filter.valid(doc)
                log.info("Document type: ${doc.getClass().getName()}")
                def docDataMap = doc.getDataAsMap()
                docDataMap['controlNumber'] = recordNumber
                doc.withData(docDataMap)
                doc.addIdentifier("/"+dataset+"/"+recordNumber)
                whelk.add(doc, true)
                log.debug("Added identifier /${dataset}/${recordNumber} to document ${doc.identifier}")
            } catch (Exception e) {
                log.error("Tried to get controlNumber from APIX response and failed: ${e.message}", e)
            }
        }
    }
}

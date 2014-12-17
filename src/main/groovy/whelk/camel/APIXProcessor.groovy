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

        log.debug("processing ${message.getHeader('document:identifier')} for APIX")
        log.debug("Received operation: " + operation)
        log.debug("dataset: ${message.getHeader('document:dataset')}")
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
                String voyagerUri = getVoyagerUri(message.getHeader("whelk:identifier"), message.getHeader("whelk:dataset"), message.getHeader("whelk:controlNumber")) ?: "/" + message.getHeader("document:dataset") +"/new"
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
        } else {
            if (!messagePrepared) {
                log.info("CALLING CREATE DOCUMENT FROM APIXPROCESSOR")
                def doc = createDocument(message)
                String voyagerUri = getVoyagerUri(doc) ?: "/" + message.getHeader("document:dataset") +"/new"
                doc = runConverters(doc)
                prepareMessage(doc, message)

                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }

        exchange.setOut(message)
    }


    String getVoyagerUri(String xlIdentifier, String dataset, String controlNumber) {
        if (xlIdentifier ==~ /\/(auth|bib|hold)\/\d+/) {
            log.debug("Identified apix uri: ${xlIdentifier}")
            return xlIdentifier
        }
        if (controlNumber) {
            log.debug("Constructing apix uri: /${dataset}/${controlNumber}")
            return "/"+dataset+"/"+controlNumber
        }
        log.debug("Could not assertain a voyager URI for $xlIdentifier")
        return null
    }

    String getVoyagerUri(Document doc) {
        String vUri = getVoyagerUri(doc.identifier, doc.dataset, doc.getDataAsMap().get("controlNumber"))
        log.debug("Looking in ${doc.identifiers}")
        for (altId in doc.identifiers) {
            if (altId ==~ /\/(auth|bib|hold)\/\d+/) {
                log.debug("Identified apix uri from alternates: ${altId}")
                return altId
            }
        }
        return vUri
    }
}

@Log
class APIXResponseProcessor implements Processor {

    Whelk whelk

    APIXResponseProcessor(Whelk w) {
        this.whelk = w
    }

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
                log.error("APIX responded with error code ${xmlresponse.@error_code} (${xmlresponse.@error_message}) when calling ${message.getHeader('CamelHttpPath')} for document ${message.getHeader('document:identifier')}")
            } else {
                log.info("Received XML response from APIX: $xmlBody")
            }
        } else if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && message.getHeader("CamelHttpResponseCode") == 201) {
            log.debug("Document created in APIX. Try to harvest the identifier ...")
            try {
                String recordNumber = message.getHeader("Location")?.split("/")[-1]
                String dataset = message.getHeader("Location")?.split("/")[-2]
                Document doc = whelk.get(message.getHeader("document:identifier"))
                // Is this necessary?
                def docDataMap = doc.getDataAsMap()
                docDataMap['controlNumber'] = recordNumber
                doc.withData(docDataMap)
                // Wouldn't we rather:
                doc.addIdentifier("/"+dataset+"/"+recordNumber)
                whelk.add(doc, true)
                log.debug("Added identifier /${dataset}/${recordNumber} to document ${doc.identifier}")
            } catch (Exception e) {
                log.error("Tried to get controlNumber from APIX response and failed: ${e.message}", e)
            }
        }
    }
}

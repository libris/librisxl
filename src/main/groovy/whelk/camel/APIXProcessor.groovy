package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods


@Log
class APIXProcessor extends BasicPlugin implements Processor {

    String apixPathPrefix
    FormatConverterProcessor fcp = null

    APIXProcessor(String prefix) {
        StringBuilder pathBuilder = new StringBuilder(prefix)
        while (pathBuilder[-1] == '/') {
            pathBuilder.deleteCharAt(pathBuilder.length()-1)
        }
        this.apixPathPrefix = pathBuilder.toString()
    }

    void bootstrap() {
        fcp = getPlugin("camel_format_processor")
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
                def doc = fcp.createDocument(message)
                String voyagerUri
                if (doc instanceof JsonDocument) {
                    voyagerUri = getVoyagerUri(doc)
                } else {
                    voyagerUri = getVoyagerUri(message.getHeader("whelk:identifier"), message.getHeader("whelk:dataset"))
                }
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
                if (doc) {
                    fcp.prepareMessage(doc, message)
                }
            }
        } else {
            if (!messagePrepared) {
                def doc = fcp.createDocument(message)
                String voyagerUri = getVoyagerUri(doc) ?: "/" + message.getHeader("document:dataset") +"/new"
                doc = fcp.runConverters(doc)
                fcp.prepareMessage(doc, message)

                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }

        exchange.setOut(message)
    }


    String getVoyagerUri(String xlIdentifier, String dataset) {
        if (xlIdentifier ==~ /\/(auth|bib|hold)\/\d+/) {
            log.debug("Identified apix uri: ${xlIdentifier}")
            return xlIdentifier
        }
        log.debug("Could not assertain a voyager URI for $xlIdentifier")
        return null
    }

    String getVoyagerUri(Document doc) {
        String vUri = getVoyagerUri(doc.identifier, doc.dataset)
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
class APIXResponseProcessor extends BasicPlugin implements Processor {

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
                log.debug("APIX response: $xmlBody")
                def errorDetails = [:]
                Document failedDocument = whelk.get(message.getHeader("document:identifier"))
                def docMeta = failedDocument.meta
                docMeta['apixError'] = true
                docMeta['apixErrorCode'] = xmlresponse.@error_code as String
                docMeta['apixErrorMessage'] = xmlresponse.@error_message as String
                docMeta['apixRequestPath'] = message.getHeader('CamelHttpPath').toString()
                failedDocument.withMeta(docMeta)
                whelk.add(failedDocument, false)
            } else {
                log.info("Received XML response from APIX: $xmlBody")
            }
        } else if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && (message.getHeader("CamelHttpResponseCode") == 201 || message.getHeader("CamelHttpResponseCode") == 303)) {
            log.debug("Document created in APIX. Try to harvest the identifier ...")
            try {
                String recordNumber = message.getHeader("Location").split("/")[-1]
                String dataset = message.getHeader("Location").split("/")[-2]
                Document doc = whelk.get(message.getHeader("document:identifier"))
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

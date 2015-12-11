package whelk.integration.process

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods

import whelk.Document
import whelk.Location
import whelk.Whelk
import whelk.converter.MarcXml2JsonLDConverter
import whelk.JsonLd

import org.apache.log4j.Logger


class APIXResponseProcessor implements Processor {

    Logger logger = Logger.getLogger(APIXResponseProcessor.class.getName())

    Whelk whelk
    MarcXml2JsonLDConverter marcXml2JsonLDConverter

    APIXResponseProcessor(Whelk whelk) {
        this.whelk = whelk
        marcXml2JsonLDConverter = new MarcXml2JsonLDConverter()
    }

    @Override
    public void process(Exchange exchange) throws Exception {

        Message message = exchange.getIn()

        if (logger.isTraceEnabled()) {
            message.getHeaders().each { key, value ->
                logger.trace("APIX response message header: $key = $value")
            }
        }

        logger.info("APIX reponse code: ${message.getHeader('CamelHttpResponseCode')} for ${message.getHeader('CamelHttpMethod')} ${message.getHeader('CamelHttpPath')}")

        if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && message.getHeader("CamelHttpResponseCode") == 200) {

            String xmlBody = message.getBody(String.class)
            def xmlresponse = new XmlSlurper(false,false).parseText(xmlBody)

            if (xmlresponse.@status == "ERROR") {

                logger.error("APIX responded with error code ${xmlresponse.@error_code} (${xmlresponse.@error_message}) when calling ${message.getHeader('CamelHttpPath')} for document ${message.getHeader('document:identifier')}")
                logger.debug("APIX response: $xmlBody")

                Location location = whelk.storage.locate(message.getHeader("document:identifier"), true)

                Document failedDocument = location?.document

                def docManifest = failedDocument.manifest ?: null
                docManifest['apixError'] = true
                docManifest['apixErrorCode'] = xmlresponse.@error_code as String
                docManifest['apixErrorMessage'] = xmlresponse.@error_message as String
                docManifest['apixRequestPath'] = message.getHeader('CamelHttpPath').toString()
                failedDocument.withManifest(docManifest)

                if (failedDocument.contentType == "application/marcxml+xml") {

                    logger.debug("Converting document from marcxml+xml to ld+json...")
                    failedDocument = marcXml2JsonLDConverter.doConvert(failedDocument)

                    logger.debug("Flattening ${failedDocument.id}")
                    failedDocument.data = JsonLd.flatten(failedDocument.data)

                }
                whelk.storage.store(failedDocument)
                whelk.elastic.index(failedDocument)

                if (xmlresponse.@error_message.toString().endsWith(" error = 203") && xmlresponse.@error_code.toString() == "2") {
                    message.setHeader("retry", true)
                    logger.info("Setting retry with next: ${message.getHeader('JMSDestination')}")
                    message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))
                } else {
                    logger.info("Error ${xmlresponse.@error_code} (${xmlresponse.@error_message}) is not deemed retryable.")
                }
            } else {
                logger.info("Received XML response from APIX: $xmlBody")
            }

        } else if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && (message.getHeader("CamelHttpResponseCode") == 201 || message.getHeader("CamelHttpResponseCode") == 303)) {

            logger.debug("Document created in APIX. Try to harvest the identifier ...")

            try {

                String recordNumber = message.getHeader("Location").split("/")[-1]
                String dataset = message.getHeader("Location").split("/")[-2]

                Document doc = whelk.storage.locate(message.getHeader("document:identifier"), true).document
                logger.info("Document type: ${doc.getClass().getName()}")

                def docDataMap = doc.data
                docDataMap['controlNumber'] = recordNumber
                doc.withData(docDataMap)
                doc.addIdentifier("/"+dataset+"/"+recordNumber)

                if (doc.contentType == "application/ld+json") {
                    logger.debug("Flattening ${doc.id}")
                    doc.data = JsonLd.flatten(doc.data)
                }
                whelk.storage.store(doc)
                whelk.elastic.index(doc)

                logger.debug("Added identifier /${dataset}/${recordNumber} to document ${doc.identifier}")

            } catch (Exception e) {
                logger.error("Tried to get controlNumber from APIX response and failed: ${e.message}", e)
            }
        }
    }
}

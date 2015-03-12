package whelk.camel

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*
import whelk.exception.*

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods


@Log
class APIXProcessor extends BasicPlugin implements Processor {

    String apixPathPrefix
    List<FormatConverter> converters = []
    List<Filter> filters = []

    APIXProcessor(String prefix) {
        StringBuilder pathBuilder = new StringBuilder(prefix)
        while (pathBuilder[-1] == '/') {
            pathBuilder.deleteCharAt(pathBuilder.length()-1)
        }
        this.apixPathPrefix = pathBuilder.toString()
    }

    void bootstrap() {
        converters = plugins.findAll { it instanceof FormatConverter }
        filters = plugins.findAll { it instanceof Filter }
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        println "APIX start processing."
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
                def doc = createDocument(message)
                log.debug("Recreated document in preparation for deletion. (${doc?.identifier})")
                String voyagerUri
                if (doc instanceof JsonDocument) {
                    voyagerUri = getVoyagerUri(doc)
                    log.debug("got voyageruri $voyagerUri from doc")
                } else {
                    voyagerUri = getVoyagerUri(message.getHeader("document:identifier"), message.getHeader("document:dataset"))
                    log.debug("got voyageruri $voyagerUri from headers")
                }
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
                if (doc) {
                    prepareMessage(doc, message)
                }
            }
        } else {
            if (!messagePrepared) {
                def doc = createDocument(message)
                String voyagerUri = getVoyagerUri(doc)
                if (!voyagerUri) {
                    if (message.getHeader("document:dataset") == "hold") {
                        voyagerUri = getUriForNewHolding(doc)
                    } else {
                        voyagerUri =  "/" + message.getHeader("document:dataset") +"/new"
                    }
                }
                doc = runConverters(doc)
                prepareMessage(doc, message)

                log.debug("PUT to APIX at ${apixPathPrefix + voyagerUri}")
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }
        log.debug("Sending message to ${message.getHeader(Exchange.HTTP_PATH)}")

        exchange.setOut(message)
    }

    String getUriForNewHolding(Document doc) {
        log.debug("Constructing proper URI for creating new holding.")
        def holdingFor = doc.dataAsMap.about.holdingFor.get("@id")
        log.debug("Document is holding for $holdingFor, loading that document ...")
        def location = whelk.locate(holdingFor)
        log.debug("Found location: $location")
        def bibDoc = location?.document
        log.debug("It contained a doc: $bibDoc")
        if (!bibDoc && location) {
            log.debug(" ... or rather not. Loading redirect: ${location.uri}")
            bibDoc = whelk.get(location.uri.toString())
        }
        String apixNewHold = null
        try {
            apixNewHold = getVoyagerUri(bibDoc) + "/newhold"
            if (!apixNewHold) {
                throw new Exception("Failed")
            }
        } catch (Exception e) {
            throw new IdentifierException("Could not figure out voyager-id for document ${doc.identifier}")
        }
        log.debug("Constructed URI $apixNewHold for creating holding.")
        return apixNewHold
    }

    String getVoyagerUri(String xlIdentifier, String dataset) {
        log.debug("trying to build voyager uri from $xlIdentifier and $dataset")
        if (xlIdentifier ==~ /\/(auth|bib|hold)\/\d+/) {
            log.debug("Identified apix uri: ${xlIdentifier}")
            return xlIdentifier
        }
        log.debug("Could not assertain a voyager URI for $xlIdentifier")
        return null
    }

    String getVoyagerUri(Document doc) {
        String vUri = getVoyagerUri(doc.identifier, doc.dataset)
        log.debug("proposed voy id from identifier: $vUri")
        log.debug("Looking in ${doc.identifiers}")
        for (altId in doc.identifiers) {
            if (altId ==~ /\/(auth|bib|hold)\/\d+/) {
                log.debug("Identified apix uri from alternates: ${altId}")
                return altId
            }
        }
        return vUri
    }

    Document createDocument(Message docMessage) {
        def body = docMessage.getBody()
        Document doc
        if (body instanceof String) {
            doc = whelk.get(docMessage.getBody())
            log.debug("Loaded document ${doc?.identifier}")
        } else {
            log.debug("Setting document data with type ${body.getClass().getName()}")
            def metaentry = mapper.readValue(docMessage.getHeader("document:metaentry") as String, Map)
            doc = whelk.createDocument(metaentry.entry.contentType).withData(body).withMeta(metaentry.meta).withEntry(metaentry.entry)
        }
        return doc
    }

    Document runConverters(Document doc) {
        log.debug("converters: $converters filters: $filters")
        if (doc && (converters || filters)) {
            for (converter in converters) {
                log.debug("Running converter ${converter.id}.")
                doc = converter.convert(doc)
            }
            for (filter in filters) {
                log.debug("Running filter ${filter.id}.")
                doc = filter.filter(doc)
            }
        }
        return doc
    }

    void prepareMessage(Document doc, Message docMessage) {
        log.debug("Resetting document ${doc.identifier} in message.")
        docMessage.setBody(doc.data)
        docMessage.setHeader("document:metaentry", doc.metadataAsJson)
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

package whelk.camel

import groovy.transform.Synchronized
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

    static final int MAX_RETRY_COUNT = 60

    APIXProcessor(String prefix) {
        StringBuilder pathBuilder = new StringBuilder(prefix)
        while (pathBuilder[-1] == '/') {
            pathBuilder.deleteCharAt(pathBuilder.length()-1)
        }
        this.apixPathPrefix = pathBuilder.toString()
    }

    static Set queuedIds = new HashSet<String>()

    void bootstrap() {
        converters = plugins.findAll { it instanceof FormatConverter }
        filters = plugins.findAll { it instanceof Filter }
    }

    @Synchronized
    static isIdQueued(String id) {
        return queuedIds.contains(id)
    }

    @Synchronized
    static addIdToQueue(String id) {
        println("Adding $id to queue")
        queuedIds.add(id)
    }

    @Synchronized
    static removeIdFromQueue(String id) {
        println("Removing $id from queue")
        queuedIds.remove(id)
    }


    @Override
    public void process(Exchange exchange) throws Exception {
        log.debug("APIX start processing.")
        Message message = exchange.getIn()

        // Remove retry-header to prevent looping
        message.removeHeader("retry")

        String operation = message.getHeader("whelk:operation")

        log.trace("processing ${message.getHeader('document:identifier')} for APIX")
        log.trace("Received operation: " + operation)
        log.trace("dataset: ${message.getHeader('document:dataset')}")
        log.trace("update: ${message.getHeader('whelk:update')}")

        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")

        String identifier = message.getHeader('document:identifier')
        log.debug("Loading fresh document from storage for id ${identifier}")

        Document doc = whelk.get(identifier)
        String voyagerUri = getVoyagerUri(doc)
        int retryCount = message.getHeader("retryCount", -1) + 1

        if ((message.getHeader('whelk:update')?.toString() == "true" || isIdQueued(identifier)) &&
                voyagerUri == null &&
                (retryCount < MAX_RETRY_COUNT)) {

            log.debug("Sending message $identifier to retry queue.")
            message.setHeader("retry", true)
            message.setHeader("retryCount", retryCount)

        } else {
            // If we get here, any remaining retry-count should be reset. Failures after this point should trigger a new retry-loop.
            message.removeHeader("retryCount")
            if (!voyagerUri) {
                log.debug("Setting paths to create new document.")
                if (message.getHeader("document:dataset") == "hold") {
                    voyagerUri = getUriForNewHolding(doc)
                } else {
                    voyagerUri = "/" + message.getHeader("document:dataset") + "/new"
                }
            }
            if (doc.isDeleted()) {
                log.debug("Document is deleted")
                message.setHeader(Exchange.HTTP_METHOD, HttpMethods.DELETE)
            } else {
                doc = runConverters(doc)
                log.trace("Setting method PUT to APIX")

                message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
                addIdToQueue(identifier)
            }
            prepareMessage(doc, message)
            log.trace("Setting path: ${apixPathPrefix + voyagerUri}")
            message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
        }

        log.trace("Sending message to ${message.getHeader(Exchange.HTTP_PATH)}")

        exchange.setOut(message)
    }

    String getUriForNewHolding(Document doc) {
        log.trace("Constructing proper URI for creating new holding.")
        def holdingFor = doc.dataAsMap.about.holdingFor.get("@id")
        log.trace("Document is holding for $holdingFor, loading that document ...")
        def location = whelk.locate(holdingFor)
        log.trace("Found location: $location")
        def bibDoc = location?.document
        log.trace("It contained a doc: $bibDoc")
        if (!bibDoc && location) {
            log.trace(" ... or rather not. Loading redirect: ${location.uri}")
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
        log.trace("Constructed URI $apixNewHold for creating holding.")
        return apixNewHold
    }

    String getVoyagerUri(String xlIdentifier, String dataset) {
        log.trace("trying to build voyager uri from $xlIdentifier and $dataset")
        if (xlIdentifier ==~ /\/(auth|bib|hold)\/\d+/) {
            log.trace("Identified apix uri: ${xlIdentifier}")
            return xlIdentifier
        }
        log.trace("Could not assertain a voyager URI for $xlIdentifier")
        return null
    }

    String getVoyagerUri(Document doc) {
        String vUri = getVoyagerUri(doc.identifier, doc.dataset)
        log.trace("proposed voy id from identifier: $vUri")
        log.trace("Looking in ${doc.identifiers}")
        for (altId in doc.identifiers) {
            if (altId ==~ /\/(auth|bib|hold)\/\d+/) {
                log.trace("Identified apix uri from alternates: ${altId}")
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
            log.trace("Loaded document ${doc?.identifier}")
        } else {
            log.trace("Setting document data with type ${body.getClass().getName()}")
            def metaentry = mapper.readValue(docMessage.getHeader("document:metaentry") as String, Map)
            doc = whelk.createDocument(metaentry.entry.contentType).withData(body).withMeta(metaentry.meta).withEntry(metaentry.entry)
        }
        return doc
    }

    Document runConverters(Document doc) {
        log.trace("converters: $converters filters: $filters")
        if (doc && (converters || filters)) {
            for (converter in converters) {
                log.trace("Running converter ${converter.id}.")
                doc = converter.convert(doc)
            }
            for (filter in filters) {
                log.trace("Running filter ${filter.id}.")
                doc = filter.filter(doc)
            }
        }
        return doc
    }

    void prepareMessage(Document doc, Message docMessage) {
        log.trace("Resetting document ${doc.identifier} in message.")
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
        log.debug("APIX reponse code: ${message.getHeader('CamelHttpResponseCode')} for ${message.getHeader('CamelHttpMethod')} ${message.getHeader('CamelHttpPath')}")
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
                    //log.info("Setting retry with next: ${message.getHeader('JMSDestination')}")
                    //message.setHeader("next", message.getHeader("JMSDestination").toString().replace("queue://", "activemq:"))
                } else {
                    log.info("Error ${xmlresponse.@error_code} (${xmlresponse.@error_message}) is not deemed retryable.")
                    APIXProcessor.removeIdFromQueue(doc.identifier)
                }
            } else {
                log.info("Received XML response from APIX: $xmlBody")
                APIXProcessor.removeIdFromQueue(doc.identifier)
            }
        } else if (message.getHeader("CamelHttpMethod") == HttpMethods.PUT && (message.getHeader("CamelHttpResponseCode") == 201 || message.getHeader("CamelHttpResponseCode") == 303)) {
            log.debug("Document created in APIX. Try to harvest the identifier ...")
            Document doc = whelk.get(message.getHeader("document:identifier"))
            try {
                String recordNumber = message.getHeader("Location").split("/")[-1]
                String dataset = message.getHeader("Location").split("/")[-2]
                log.trace("Document type: ${doc.getClass().getName()}")
                log.debug("Harvested identifier: ${dataset}/${recordNumber}")
                def docDataMap = doc.getDataAsMap()
                docDataMap['controlNumber'] = recordNumber
                doc.withData(docDataMap)
                doc.addIdentifier("/"+dataset+"/"+recordNumber)
                whelk.add(doc, true, true, true)
                log.debug("Added identifier /${dataset}/${recordNumber} to document ${doc.identifier}")
            } catch (Exception e) {
                log.error("Tried to get controlNumber from APIX response and failed: ${e.message}", e)
            } finally {
                APIXProcessor.removeIdFromQueue(doc.identifier)
            }
        }
    }
}

package whelk.integration.process

import groovy.util.logging.Slf4j as Log

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods

import whelk.Document
import whelk.JsonDocument //Should be in core?


@Log
class APIXProcessor implements org.apache.camel.Processor {

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
        log.info("APIX start processing.")
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
        if (operation == "DELETE") {
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
        //TODO: don't use whelk.locate and location (document and uri)
        def location = whelk.locate(holdingFor)
        log.debug("Found location: $location")
        def bibDoc = location?.document
        log.debug("It contained a doc: $bibDoc")
        if (!bibDoc && location) {
            log.debug(" ... or rather not. Loading redirect: ${location.uri}")
            //TODO: storage.load(identifier, version=null?)
            bibDoc = whelk.get(location.uri.toString())
        }
        String apixNewHold = null
        try {
            apixNewHold = getVoyagerUri(bibDoc) + "/newhold"
            if (!apixNewHold) {
                throw new Exception("Failed")
            }
        } catch (Exception e) {
            throw new Exception("Could not figure out voyager-id for document ${doc.identifier}")
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
            //TODO: storage.load(identifier)
            doc = whelk.get(docMessage.getBody()) //docMessage.getBody returnerar identifier?
            //TODO: apply filters, if filter.valid(doc)
            log.debug("Loaded document ${doc?.identifier}")
        } else {
            log.debug("Setting document data with type ${body.getClass().getName()}")
            def metaentry = mapper.readValue(docMessage.getHeader("document:metaentry") as String, Map)
            doc = whelk.createDocument(metaentry.entry.contentType).withData(body).withMeta(metaentry.meta).withEntry(metaentry.entry)
        }
        return doc
    }

    Document runConverters(Document doc) {
        //TODO: provide converters and filters
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

package whelk.integration.process

import org.apache.log4j.Logger

import org.apache.camel.Exchange
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.component.http4.HttpMethods

import whelk.Document
import whelk.Location
import whelk.Whelk
import whelk.converter.marc.JsonLD2MarcXMLConverter


class APIXProcessor implements org.apache.camel.Processor {

    Logger logger = Logger.getLogger(APIXProcessor.class.getName())

    Whelk whelk
    JsonLD2MarcXMLConverter jsonLD2MarcXMLConverter
    String apixPathPrefix

    APIXProcessor(String prefix, Whelk whelk, JsonLD2MarcXMLConverter jsonLD2MarcXMLConverter) { //prefix = /apix/0.1/cat/libris/

        this.whelk = whelk
        this.jsonLD2MarcXMLConverter = jsonLD2MarcXMLConverter

        StringBuilder pathBuilder = new StringBuilder(prefix)
        while (pathBuilder[-1] == '/') {
            pathBuilder.deleteCharAt(pathBuilder.length()-1)
        }

        this.apixPathPrefix = pathBuilder.toString()

    }

    @Override
    public void process(Exchange exchange) throws Exception {

        logger.info("APIX start processing.")

        Message message = exchange.getIn()

        Map messageBody = message.getBody()

        String operation = messageBody["info"]["operation"]
        String id = messageBody["info"]["id"]
        Map documentData = messageBody["documentData"]
        Map metaData = messageBody["metaData"]

        logger.debug("Processing document " + id + " for APIX")
        logger.debug("Operation: " + operation)

        boolean messagePrepared = false

        if (message.getHeader("CamelHttpPath")) {
            logger.debug("Message is already prepped ... Forego all treatment.")
            message.setHeader(Exchange.HTTP_PATH, message.getHeader("CamelHttpPath"))
            messagePrepared = true
        }

        message.setHeader(Exchange.CONTENT_TYPE, "application/xml")
        if (operation == "DELETE") {
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.DELETE)

            if (!messagePrepared) {

                Document doc = new Document().withIdentifier(id).withData(documentData).withManifest(metaData)
                logger.debug("Recreated document in preparation for deletion. (${doc?.identifier})")

                String voyagerUri = getVoyagerUri(message.getHeader("document:identifier"))

                logger.debug("DELETE to APIX at ${apixPathPrefix + voyagerUri}")
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)

                if (doc) {
                    message.setBody(doc.data)
                    message.setHeader("document:metaentry", doc.manifestAsJson)
                }
            }
        } else {

            if (!messagePrepared) {

                Document doc = new Document().withIdentifier(id).withData(documentData).withManifest(metaData)

                String voyagerUri = getVoyagerUri(doc)
                if (!voyagerUri) {
                    if (message.getHeader("document:dataset") == "hold") {
                        voyagerUri = getUriForNewHolding(doc)
                    } else {
                        voyagerUri =  "/" + message.getHeader("document:dataset") +"/new"
                    }
                }
                doc = jsonLD2MarcXMLConverter.convert(doc) //TODO: other converters?

                message.setBody(doc.data)
                message.setHeader("document:metaentry", doc.manifestAsJson)

                logger.debug("PUT to APIX at ${apixPathPrefix + voyagerUri}")
                message.setHeader(Exchange.HTTP_PATH, apixPathPrefix + voyagerUri)
            }
            message.setHeader(Exchange.HTTP_METHOD, HttpMethods.PUT)
        }

        logger.debug("Sending message to ${message.getHeader(Exchange.HTTP_PATH)}")

        exchange.setOut(message)
    }


    String getUriForNewHolding(Document doc) {

        String holdingFor = doc.data.about.holdingFor.get("@id")
        logger.debug("Document is holding for $holdingFor, loading that document ...")

        Location location = whelk.locate(holdingFor)
        logger.debug("Found location: $location")

        Document bibDoc = location?.document
        logger.debug("It contained a doc: $bibDoc")

        if (!bibDoc && location) {
            logger.debug(" ... or rather not. Loading redirect: ${location.uri}")
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

        logger.debug("Constructed URI $apixNewHold for creating holding.")

        return apixNewHold
    }


    String getVoyagerUri(String xlIdentifier) {

        if (xlIdentifier ==~ /\/(auth|bib|hold)\/\d+/) {

            logger.debug("Identified apix uri: ${xlIdentifier}")
            return xlIdentifier
        }

        logger.debug("Could not assertain a voyager URI for $xlIdentifier")
        return null
    }


    String getVoyagerUri(Document doc) {

        String vUri = getVoyagerUri(doc.identifier, doc.dataset)

        logger.debug("proposed voy id from identifier: $vUri")
        logger.debug("Looking in ${doc.identifiers}")

        for (altId in doc.identifiers) {

            if (altId ==~ /\/(auth|bib|hold)\/\d+/) {
                logger.debug("Identified apix uri from alternates: ${altId}")
                return altId
            }
        }

        return vUri
    }


    /*Document runConverters(Document doc) {

        logger.debug("converters: $converters filters: $filters")

        if (doc && (converters || filters)) {

            for (converter in converters) {
                logger.debug("Running converter ${converter.id}.")
                doc = converter.convert(doc)
            }

            for (filter in filters) {
                logger.debug("Running filter ${filter.id}.")
                doc = filter.filter(doc)
            }
        }

        return doc
    }*/

}

package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.resource.*
import org.restlet.representation.*

import org.codehaus.jackson.map.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.utils.isbn.*
import se.kb.libris.conch.Tools
import se.kb.libris.whelks.http.*


@Log
class FormatApiRestlet extends BasicWhelkAPI {
    def pathEnd = "_format"
    String id = "formatapi"
    def varPath = false
    String description = "API to transform between formats the whelk is capable of handling."

    void doHandle(Request request, Response response) {
        def reqMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        if (request.method == Method.POST) {
            String requestedContentType = reqMap.get("to")
            Document doc = new Document().withData(Tools.normalizeString(request.entityAsText)).withEntry(["contentType":request.entity.mediaType.toString()])
            if (requestedContentType) {
                log.info("Constructed document. Asking for converter for $requestedContentType")
                def fc = this.whelk.formatConverters.find { it.value.requiredContentType == doc.contentType && it.value.resultContentType == requestedContentType }.value
                if (fc) {
                    log.info("Foound converter: ${fc.id}")
                    doc = fc.convert(doc)
                } else {
                    log.error("No formatconverter found for $requestedContentType")
                    response.setStatus(Status.CLIENT_ERROR_UNSUPPORTED_MEDIA_TYPE)
                }
            } else {
                log.info("No conversion requested. Returning document as is.")
            }
            response.setEntity(doc.dataAsString, LibrisXLMediaType.getMainMediaType(doc.contentType))
        } else {
            response.setStatus(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED)
        }
    }
}


@Log
class HoldCounter extends SearchRestlet {
    def pathEnd = "_libcount"
    def varPath = false
    String id = "HoldingsCounter"
    String description = "Custom search API for counting holdings."

    HoldCounter(indexTypeConfig) {
        super(indexTypeConfig)
    }

    @Override
    void doHandle(Request request, Response response) {
        def queryMap = request.getResourceRef().getQueryAsForm().getValuesMap()
        def idparam = queryMap.get("id")
        log.info("idparam: $idparam")

        def elasticQuery = new ElasticQuery("about.annotates.@id", idparam)
        elasticQuery.indexType = config.defaultIndexType
        elasticQuery.n = 0

        try {
            response.setEntity(performQuery(elasticQuery), MediaType.APPLICATION_JSON)
        } catch (WhelkRuntimeException wrte) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)

        }
    }
}

@Log
class ISXNTool extends BasicWhelkAPI implements WhelkAware {
    def pathEnd = "_isxntool"
    String id = "ISXNTool"
    String description = "Formats data (ISBN-numbers) according to international presention rules."
    Whelk dataWhelk
    ObjectMapper mapper = new ObjectMapper()

    /*
    ISXNTool(Whelk dw) {
        this.dataWhelk = dw
    }
    */

    void doHandle(Request request, Response response) {
        def querymap = request.getResourceRef().getQueryAsForm().getValuesMap()
        String isbnString = querymap.get("isbn")
        if (isbnString) {
            handleIsbn(isbnString, querymap, request, response)
        } else {
            response.setEntity('{"error":"No valid parameter found."}', MediaType.APPLICATION_JSON)
        }
    }

    void handleIsbn(String isbnString, Map querymap, Request request, Response response) {
        String providedIsbn = isbnString
        isbnString = isbnString.replaceAll(/[^\dxX]/, "")
        def isbnmap = [:]
        isbnmap["provided"] = providedIsbn
        Isbn isbn = null
        try {
            isbn = IsbnParser.parse(isbnString)
        }  catch (se.kb.libris.utils.isbn.IsbnException iobe) {
            log.debug("Validation of isbn $isbnString failed: wrong length")
        }
        if (isbn) {
            log.debug("isbnString: $isbnString")
            String formattedIsbn = isbn.toString(true)
            String properIsbn = isbn.toString()
            boolean checkExists = (querymap.get("check", "false") == "true")
            boolean ignoreValid = (querymap.get("ignoreValidation", "false") == "true")
            boolean isValid = validISBN(isbnString)
            isbnmap["valid"] = isValid
            if (ignoreValid || isValid) {
                isbnmap["formatted"] = formattedIsbn
                isbnmap["proper"] = properIsbn
            }
            if (checkExists) {
                def results = dataWhelk.search(new Query(isbn.toString()).addField("about.isbn"))
                isbnmap["exists"] = (results.numberOfHits > 0)
            }
        } else {
            isbnmap["valid"] = false
            isbnmap["error"] = new String("Failed to parse $providedIsbn as ISBN")
        }
        response.setEntity(mapper.writeValueAsString(["isbn":isbnmap]), MediaType.APPLICATION_JSON)
    }

    boolean validISBN(String isbn) {
        boolean valid = false
        int n = 0
        try {
            // calculate sum
            if (isbn.length() == 10) {
                for (int i=0;i<isbn.length()-1;i++) {
                    n += Character.getNumericValue(isbn.charAt(i))*Isbn.weights[i];
                }
                n %= 11;
                valid = (isbn.charAt(9) == (n == 10 ? 'X' : (""+n).charAt(0)))
                log.debug("isbn10 check digit: $n ($valid)")
            } else if (isbn.length() == 13) {
                for (int i=0;i<isbn.length()-1;i++)
                n += Character.getNumericValue(isbn.charAt(i))*Isbn.weights13[i];
                n = (10 - (n % 10)) % 10;
                valid = (isbn.charAt(12) == (""+n).charAt(0))
                log.debug("isbn13 check digit: $n ($valid)")
            }
        } catch (Exception e) {
            return valid
        }
        //return (n==10)? 'X' : (char)(n + '0');
        return valid
    }
}

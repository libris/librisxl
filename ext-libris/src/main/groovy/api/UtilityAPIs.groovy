package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import se.kb.libris.conch.Tools

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.utils.isbn.*

@Log
class FormatterAPI extends BasicAPI {

    String description = "API to transform between formats the whelk is capable of handling."
    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        if (request.method == "POST") {
            String requestedContentType = request.getParameter("to")
            if (request.getContentLength() == 0) {
                log.warn("[${this.id}] Received no content to reformat.")
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.")
            } else {
                Document doc = new Document().withData(Tools.normalizeString(request.getInputStream().getText("UTF-8"))).withEntry(["contentType":request.contentType, "timestamp": new Date().getTime()])
                if (requestedContentType) {
                    log.info("Constructed document. Asking for converter for $requestedContentType")
                    def fc = plugins.find { it.requiredContentType == doc.contentType && it.resultContentType == requestedContentType }
                    if (fc) {
                        log.info("Found converter: ${fc.id}")
                        doc = fc.convert(doc)
                    } else {
                        log.error("No formatconverter found to convert from ${doc.contentType} to $requestedContentType")
                        response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE)
                    }
                } else {
                    log.info("No conversion requested. Returning document as is.")
                }
                sendResponse(response, doc.dataAsString, doc.contentType)
            }
        } else {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
    }
}

@Log
class HoldCounter extends SearchAPI {
    String description = "Custom search API for counting holdings."

    HoldCounter(indexTypeConfig) {
        super(indexTypeConfig)
    }

    @Override
    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def idparam = request.getParameter("id")
        log.info("idparam: $idparam")

        //def elasticQuery = new ElasticQuery("about.annotates.@id", idparam)
        def elasticQuery = new ElasticQuery(["terms":["about.annotates.@id:${idparam}"]])
        elasticQuery.indexTypes = [config.defaultIndexType]
        elasticQuery.n = 0

        try {
            sendResponse(response, performQuery(elasticQuery, null), "application/json")
        } catch (WhelkRuntimeException wrte) {
            response.sendError(Status.SC_BAD_REQUEST, wrte.message)

        }
    }
}

@Log
class ISXNTool extends BasicAPI {
    String description = "Formats data (ISBN-numbers) according to international presention rules."

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        if (request.getParameter("isbn")) {
            handleIsbn(request, response)
        } else {
            sendResponse(response, '{"error":"No valid parameter found."}', "application/json")
        }
    }

    void handleIsbn(HttpServletRequest request, HttpServletResponse response) {
        String providedIsbn = request.getParameter("isbn")
        String isbnString = providedIsbn.replaceAll(/[^\dxX]/, "")
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
            boolean checkExists = (request.getParameter("check") == "true")
            boolean ignoreValid = (request.getParameter("ignoreValidation") == "true")
            boolean isValid = validISBN(isbnString)
            isbnmap["valid"] = isValid
            if (ignoreValid || isValid) {
                isbnmap["formatted"] = formattedIsbn
                isbnmap["proper"] = properIsbn
            }
            if (checkExists) {
                def results = whelk.search(new Query(isbn.toString()).addField("about.isbn"))
                isbnmap["exists"] = (results.numberOfHits > 0)
            }
        } else {
            isbnmap["valid"] = false
            isbnmap["error"] = new String("Failed to parse $providedIsbn as ISBN")
        }
        sendResponse(response, mapper.writeValueAsString(["isbn":isbnmap]), "application/json")
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

@Log
class CompleteExpander extends BasicAPI {
    String description = "Provides useful information about authorities."

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def identifier, result, relator, authDoc, resultMap, authDataMap, idQuery

        if (pathVars.size() == 0) {
            response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST)
        } else {
            identifier = pathVars.first()
            authDoc = whelk.get(new URI(identifier))
            if (!authDoc) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND)
            } else {
                authDataMap = authDoc.dataAsMap
                idQuery = "\\/resource\\/" + identifier.replace("/", "\\/")

                if (authDataMap.about."@type" == "Person") {
                    result = whelk.search(new ElasticQuery(idQuery).addField("about.instanceOf.attributedTo.@id").addFacet("about.@type"))
                    relator = "attributedTo"
                    if (result.numberOfHits == 0) {
                        result = whelk.search(new ElasticQuery(idQuery).addField("about.instanceOf.influencedBy.@id").addFacet("about.@type"))
                        relator = "influencedBy"
                    }
                    if (result.numberOfHits > 0) {
                        resultMap = result.toMap(["about.@type", "about.title.titleValue", "originalCatalogingAgency.name", "function", "exampleTitle"])
                        resultMap.list.eachWithIndex() { r, i ->
                            if (resultMap.list[i].get("data", null)) {
                                resultMap.list[i].data["function"] = relator
                                //TODO: number of holds
                                if (relator.equals("attributedTo")) {
                                    resultMap["extraKnowledge"] = ["exampleTitle" : resultMap.list[i].data.about.title.titleValue]
                                }
                            }
                        }
                    }

                }  else if (authDataMap.about."type" == "Concept") {
                    //TODO
                }

                if (!resultMap) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND)
                } else {
                    sendResponse(response, mapper.writeValueAsString(resultMap), "application/json")
                }

            }
        }
    }
}

package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import se.kb.libris.conch.Tools

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

@Log
class FormatterAPI extends BasicAPI {

    String id = "formatterapi"
    String description = "API to transform between formats the whelk is capable of handling."

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        if (request.method == "POST") {
            String requestedContentType = request.getParameter("to")
            if (request.getContentLength() == 0) {
                log.warn("[${this.id}] Received no content to reformat.")
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.")
            } else {
                Document doc = new Document().withData(Tools.normalizeString(request.getInputStream().getText("UTF-8"))).withEntry(["contentType":request.contentType])
                if (requestedContentType) {
                    log.info("Constructed document. Asking for converter for $requestedContentType")
                    def fc = plugins.find { it.requiredContentType == doc.contentType && it.resultContentType == requestedContentType }
                    if (fc) {
                        log.info("Found converter: ${fc.id}")
                        doc = fc.convert(doc)
                    } else {
                        log.error("No formatconverter found for $requestedContentType")
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


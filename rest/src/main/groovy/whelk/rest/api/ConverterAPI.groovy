package whelk.rest.api

import groovy.util.logging.Slf4j as Log

import org.apache.http.entity.ContentType
import whelk.Whelk
import whelk.Document
import whelk.util.Tools
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.PropertyLoader

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Log
class ConverterAPI extends HttpServlet {

    JsonLD2MarcXMLConverter marcConverter


    public ConverterAPI() {
        log.info("Starting converterAPI ...")

        marcConverter = new JsonLD2MarcXMLConverter()

        log.info("Started ...")
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        String requestedContentType = request.getParameter("to")
        if (request.getContentLength() == 0) {
            log.warn("[${this.id}] Received no content to reformat.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.")
        } else {
            String ctype = ContentType.parse(request.getContentType()).getMimeType()
            Document doc = new Document("nodoc", [(Document.NON_JSON_CONTENT_KEY): Tools.normalizeString(request.getInputStream().getText("UTF-8"))], [(Document.CONTENT_TYPE_KEY): ctype])
            if (!requestedContentType || requestedContentType == "application/x-marcxml") {
                log.info("Constructed document. Converting to $requestedContentType")
                doc = marcConverter.convert(doc)
            } else if (requestedContentType) {
                log.info("Can not convert to $requestedContentType.")
            } else {
                log.info("No conversion requested. Returning document as is.")
            }
            HttpTools.sendResponse(response, doc.dataAsString, doc.contentType)
        }
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
}

package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import org.apache.http.entity.ContentType
import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter
import whelk.util.Tools

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Log
class ConverterAPI extends HttpServlet {

    Whelk whelk
    MarcFrameConverter marcFrameConverter

    public ConverterAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    @Override
    void init() {
        log.info("Starting converterAPI")
        if (!marcFrameConverter) {
            whelk = Whelk.createLoadedCoreWhelk()
            marcFrameConverter = whelk.createMarcFrameConverter()
        }
        log.info("Started ...")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        handleConversion(request, response)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        handleConversion(request, response)
    }

    void handleConversion(HttpServletRequest request, HttpServletResponse response) {
        if (request.getContentType() == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Missing Content-Type")
            return
        }

        String ctype = ContentType.parse(request.getContentType()).getMimeType()
        if (ctype != "application/ld+json") {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Received data in unexpected format: ${ctype}")
            return
        }
        if (request.getContentLength() == 0) {
            log.warn("Received no content to reformat.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.")
            return
        }

        String requestedContentType = request.getParameter("to") ?:
                ContentType.parse(request.getHeader("Accept")).getMimeType() ?:
                        "application/x-marc-json"

        if (requestedContentType == "application/x-marc-json") {
            String jsonText = Tools.normalizeString(request.getInputStream().getText("UTF-8"))
            Map json = marcFrameConverter.mapper.readValue(jsonText, Map)
            log.info("Constructed document. Converting to $requestedContentType")
            Document doc = new Document(json)
            if (whelk) {
                whelk.embellish(doc)
            }
            json = marcFrameConverter.runRevert(doc.data)
            def framedText = marcFrameConverter.mapper.writeValueAsString(json)
            HttpTools.sendResponse(response, framedText, requestedContentType)
        }
        else if (requestedContentType) {
            def msg = "Can not convert to $requestedContentType."
            log.info(msg)
            response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE , msg)
        } else {
            def msg = "No conversion requested."
            log.info(msg)
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, msg)
        }
    }

}

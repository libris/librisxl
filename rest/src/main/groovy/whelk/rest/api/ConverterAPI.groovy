package whelk.rest.api

import groovy.util.logging.Slf4j as Log

import org.apache.http.entity.ContentType
import whelk.Whelk
import whelk.Document
import whelk.util.Tools
import whelk.converter.marc.MarcFrameConverter
import whelk.util.PropertyLoader

import org.picocontainer.Characteristics
import org.picocontainer.PicoContainer

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Created by markus on 2016-06-02.
 */
@Log
class ConverterAPI extends HttpServlet {

    PicoContainer pico
    MarcFrameConverter marcFrameConverter

    public ConverterAPI() {
        log.info("Starting converterAPI ...")

        Properties props = PropertyLoader.loadProperties("secret")

        pico = Whelk.getPreparedComponentsContainer(props)

        pico.addComponent(new MarcFrameConverter())

        pico.start()

        marcFrameConverter = pico.getComponent(MarcFrameConverter.class)

        log.info("Started ...")
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        String requestedContentType = request.getParameter("to")
        if (request.getContentLength() == 0) {
            log.warn("[${this.id}] Received no content to reformat.")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.")
        } else {
            String ctype = ContentType.parse(request.getContentType()).getMimeType()
            Document doc = new Document([(Document.NON_JSON_CONTENT_KEY): Tools.normalizeString(request.getInputStream().getText("UTF-8"))], [(Document.CONTENT_TYPE_KEY): ctype])
            if (!requestedContentType || requestedContentType == "application/x-marcxml") {
                log.info("Constructed document. Converting to $requestedContentType")
                doc = marcFrameConverter.convert(doc)
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

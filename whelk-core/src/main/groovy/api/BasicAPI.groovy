package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
abstract class BasicAPI extends BasicPlugin implements API {

    Whelk whelk
    String logMessage = "Handled #REQUESTMETHOD# for #API_ID#"

    /**
     * For discovery API.
     */
    abstract String getDescription();

    abstract protected void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars)

    void handle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        long startTime = System.currentTimeMillis()
        doHandle(request, response, pathVars)
        /*
        def headers = request.attributes.get("org.restlet.http.headers")
        def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value ?: request.clientInfo.address
        */
        log.info(logMessage.replaceAll("#REQUESTMETHOD#", request.getMethod()).replaceAll("#API_ID#", this.id) + " from ${request.getRemoteAddr()} in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
    }

    void sendResponse(HttpServletResponse response, String text, String contentType) {
        response.setCharacterEncoding("UTF-8")
        response.setContentType(getMajorContentType(contentType))
        response.writer.write(text)
        response.writer.flush()
    }

    String getMajorContentType(String contentType) {
        return contentType.replaceAll("/[\\w]+\\+", "/")
    }
}

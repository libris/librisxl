package whelk.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import whelk.*
import whelk.plugin.*

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
        def remote_addr = request.getHeader("X-Forwarded-For") ?: request.getRemoteAddr()
        log.info(logMessage.replaceAll("#REQUESTMETHOD#", request.getMethod()).replaceAll("#API_ID#", this.id) + " from ${remote_addr} in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
    }

    void sendResponse(HttpServletResponse response, String text, String contentType, int statusCode = 200) {
        if (!text) {
            sendResponse(response, new byte[0], contentType, statusCode)
        } else {
            sendResponse(response, text.getBytes("UTF-8"), contentType, statusCode)
        }
    }

    void sendResponse(HttpServletResponse response, byte[] data, String contentType, int statusCode = 200) {
        response.setStatus(statusCode)
        if (contentType) {
            response.setContentType(contentType)
            if (contentType.startsWith("text/") || contentType.startsWith("application/")) {
                response.setCharacterEncoding("UTF-8")
            }
        }

        if (data.length > 0) {
            OutputStream out = response.getOutputStream()
            out.write(data, 0, data.length)
            //response.writer.write(text)
            //response.writer.flush()
            out.flush()
            out.close()
        }
    }

    String getMajorContentType(String contentType) {
        return contentType?.replaceAll("/[\\w]+\\+", "/")
    }
}

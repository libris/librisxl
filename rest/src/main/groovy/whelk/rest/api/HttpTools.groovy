package whelk.rest.api


import groovy.util.logging.Log4j2 as Log
import org.apache.commons.lang3.exception.ExceptionUtils
import org.codehaus.groovy.runtime.StackTraceUtils
import org.codehaus.jackson.map.ObjectMapper

import javax.servlet.http.HttpServletResponse

import static org.eclipse.jetty.http.HttpStatus.getMessage

/**
 * Created by markus on 2015-10-09.
 */
@Log
class HttpTools {

    static final ObjectMapper mapper = new ObjectMapper()

    static void sendResponse(HttpServletResponse response, Map data, String contentType, int statusCode = 200) {
        if (!data) {
            sendResponse(response, new byte[0], contentType, statusCode)
        } else {
            sendResponse(response, mapper.writeValueAsBytes(data), contentType, statusCode)
        }
    }

    static void sendResponse(HttpServletResponse response, String data, String contentType, int statusCode = 200) {
        sendResponse(response, data.getBytes("UTF-8"), contentType, statusCode)
    }

    static void sendResponse(HttpServletResponse response, byte[] data, String contentType, int statusCode = 200) {
        response.setHeader('Cache-Control', 'no-cache')
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
            out.flush()
            out.close()
        }
    }

    static void sendError(HttpServletResponse response, int statusCode, String msg, Object extraInfo = null) {
        Exception e = extraInfo instanceof Exception ? extraInfo : null
        Map extra = extraInfo instanceof Map ? extraInfo : [:]
        
        Map json = [
                "message"    : msg,
                "status_code": statusCode,
                "status"     : getMessage(statusCode)
        ] + extra

        if (statusCode >= 500 && e) {
            e = StackTraceUtils.sanitize(e)
            log.error("Internal server error: ${e.getMessage()}", e)

            // Don't include servlet container stack frames
            json.stackTrace = ExceptionUtils.getStackFrames(e).with {
                it.take(2 + it.findLastIndexOf { at -> at.contains(HttpTools.class.getPackage().getName()) })
            }.collect { it.replace('\t', '    ')}
        }

        sendResponse(response, json, "application/json", statusCode)
    }
    
    enum DisplayMode {
        DOCUMENT, META, RAW
    }
}

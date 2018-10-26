package whelk.rest.api

import org.codehaus.jackson.map.ObjectMapper

import javax.servlet.http.HttpServletResponse

/**
 * Created by markus on 2015-10-09.
 */
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

    static String getMajorContentType(String contentType) {
        if (contentType == "application/x-marc-json") {
            return "application/json"
        }
        return contentType?.replaceAll("/[\\w]+\\+", "/")
    }


    enum DisplayMode {
        DOCUMENT, META, RAW
    }
}

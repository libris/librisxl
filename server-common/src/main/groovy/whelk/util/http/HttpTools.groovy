package whelk.util.http

import groovy.util.logging.Log4j2 as Log
import org.apache.commons.lang3.exception.ExceptionUtils
import org.codehaus.groovy.runtime.StackTraceUtils
import whelk.component.PostgreSQLComponent
import whelk.exception.LinkValidationException
import whelk.exception.ModelValidationException
import whelk.exception.StorageCreateFailedException
import whelk.exception.WhelkRuntimeException

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static org.eclipse.jetty.http.HttpStatus.getMessage
import static whelk.util.Jackson.mapper

/**
 * Created by markus on 2015-10-09.
 */
@Log
class HttpTools {
    static void sendResponse(HttpServletResponse response, Map data, String contentType, int statusCode = 200) {
        if (data == null) {
            sendResponse(response, new byte[0], contentType, statusCode)
        } else {
            sendResponse(response, mapper.writeValueAsBytes(data), contentType, statusCode)
        }
    }

    static void sendResponse(HttpServletResponse response, String data, String contentType, int statusCode = 200) {
        sendResponse(response, data.getBytes("UTF-8"), contentType, statusCode)
    }

    static void sendResponse(HttpServletResponse response, byte[] data, String contentType, int statusCode = 200) {
        try {
            response.setHeader('Cache-Control', 'no-cache')
            response.setStatus(statusCode)
        } catch (Exception e) {
            // Ignore. When an error occurs, after a response has already started sending, setting these
            // results in an UnsupportedOperationException, which is not ideal. Just letting this go results
            // In large log files filled with callstacks. We can't (really) remedy the problem, because doing so
            // would require caching of the full response before we start sending any of it, which is even worse.
            // This "silences" the problem, while letting the original problem (that caused the response to fail
            // in the first place) be logged on its own.
            // This whole thing is a symptom of the disease called "using exceptions for flow control".
            return
        }
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
    
    static void sendError(HttpServletResponse response, int statusCode, String msg, Exception e = null) {
        Map extra = e instanceof BadRequestException ? e.getExtraInfo() : Collections.EMPTY_MAP
        
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

    static String getBaseUri(HttpServletRequest request) {
        String baseUri = ''

        if (request.getScheme() == 'http') {
            baseUri = request.getScheme() + '://' +
                    request.getServerName() +
                    ((request.getServerPort() == 80) ? '' : ':' + request.getServerPort()) +
                    '/'
        }

        if (request.getScheme() == 'https') {
            baseUri = request.getScheme() + '://' +
                    request.getServerName() +
                    (((request.getServerPort() == 80) || (request.getServerPort() == 443)) ? '' : ':' + request.getServerPort()) +
                    '/'
        }

        return baseUri
    }

    static int mapError(Exception e) {
        switch(e) {
            case BadRequestException:
            case ModelValidationException:
            case LinkValidationException:
                return HttpServletResponse.SC_BAD_REQUEST

            case NotFoundException:
                return HttpServletResponse.SC_NOT_FOUND

            case UnsupportedContentTypeException:
                return HttpServletResponse.SC_NOT_ACCEPTABLE

            case WhelkRuntimeException:
                return HttpServletResponse.SC_INTERNAL_SERVER_ERROR

            case PostgreSQLComponent.ConflictingHoldException:
            case StorageCreateFailedException:
                return HttpServletResponse.SC_CONFLICT

            case OtherStatusException:
                return ((OtherStatusException) e).code

            default:
                return HttpServletResponse.SC_INTERNAL_SERVER_ERROR
        }
    }

    enum DisplayMode {
        DOCUMENT, META, RAW
    }
}

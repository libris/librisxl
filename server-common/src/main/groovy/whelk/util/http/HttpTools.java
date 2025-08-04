package whelk.util.http;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.runtime.StackTraceUtils;
import whelk.component.PostgreSQLComponent;
import whelk.exception.LinkValidationException;
import whelk.exception.ModelValidationException;
import whelk.exception.StorageCreateFailedException;
import whelk.exception.WhelkRuntimeException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.eclipse.jetty.http.HttpStatus.getMessage;
import static whelk.util.Jackson.mapper;

public class HttpTools {
    private static final Logger log = LogManager.getLogger(HttpTools.class);
    public static void sendResponse(HttpServletResponse response, Map<?, ?> data, String contentType) {
        sendResponse(response, data, contentType, 200);
    }

    public static void sendResponse(HttpServletResponse response, Map<?, ?> data, String contentType, int statusCode) {
        if (data == null) {
            sendResponse(response, new byte[0], contentType, statusCode);
        } else {
            byte[] valueAsBytes;
            try {
                valueAsBytes = mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed serializing data", e);
                return;
            }
            sendResponse(response, valueAsBytes, contentType, statusCode);
        }
    }

    public static void sendResponse(HttpServletResponse response, String data, String contentType) {
        sendResponse(response, data, contentType, 200);
    }

    public static void sendResponse(HttpServletResponse response, String data, String contentType, int statusCode) {
        sendResponse(response, data.getBytes(StandardCharsets.UTF_8), contentType, statusCode);
    }

    public static void sendResponse(HttpServletResponse response, byte[] data, String contentType) {
        sendResponse(response, data, contentType, 200);
    }

    public static void sendResponse(HttpServletResponse response, byte[] data, String contentType, int statusCode) {
        try {
            response.setHeader("Cache-Control", "no-cache");
            response.setStatus(statusCode);
        } catch (Exception e) {
            // Ignore. When an error occurs, after a response has already started sending, setting these
            // results in an UnsupportedOperationException, which is not ideal. Just letting this go results
            // In large log files filled with callstacks. We can't (really) remedy the problem, because doing so
            // would require caching of the full response before we start sending any of it, which is even worse.
            // This "silences" the problem, while letting the original problem (that caused the response to fail
            // in the first place) be logged on its own.
            // This whole thing is a symptom of the disease called "using exceptions for flow control".
            return;
        }
        if (contentType != null) {
            response.setContentType(contentType);
            if (contentType.startsWith("text/") || contentType.startsWith("application/")) {
                response.setCharacterEncoding("UTF-8");
            }
        }

        if (data.length > 0) {
            try {
                OutputStream out = response.getOutputStream();
                out.write(data, 0, data.length);
                out.flush();
                out.close();
            } catch (Exception ignored) {
            }
        }
    }

    public static void sendError(HttpServletResponse response, int statusCode, String msg) {
        sendError(response, statusCode, msg, null);
    }

    public static void sendError(HttpServletResponse response, int statusCode, String msg, Exception e) {
        Map<String, Object> extra = e instanceof BadRequestException ? ((BadRequestException)e).getExtraInfo() : Collections.emptyMap();

        Map<String, Object> json = new HashMap<>();
        json.put("message", msg);
        json.put("status_code", statusCode);
        json.put("status", getMessage(statusCode));
        json.putAll(extra);

        if (statusCode >= 500 && e != null) {
            e = (Exception) StackTraceUtils.sanitize(e);
            log.error("Internal server error: " + e.getMessage(), e);

            // Don't include servlet container stack frames
            String[] stackFrames = ExceptionUtils.getStackFrames(e);
            List<String> framesList = Arrays.asList(stackFrames);
            int lastRelevantIndex = -1;
            for (int i = framesList.size() - 1; i >= 0; i--) {
                if (framesList.get(i).contains(HttpTools.class.getPackage().getName())) {
                    lastRelevantIndex = i;
                    break;
                }
            }
            List<String> relevantFrames = framesList.subList(0, Math.min(framesList.size(), lastRelevantIndex + 2));
            List<String> cleanedFrames = relevantFrames.stream()
                .map(frame -> frame.replace("\t", "    "))
                .collect(Collectors.toList());
            json.put("stackTrace", cleanedFrames);
        }

        sendResponse(response, json, "application/json", statusCode);
    }

    public static String getBaseUri(HttpServletRequest request) {
        String baseUri = "";

        if ("http".equals(request.getScheme())) {
            baseUri = request.getScheme() + "://" +
                    request.getServerName() +
                    ((request.getServerPort() == 80) ? "" : ":" + request.getServerPort()) +
                    "/";
        }

        if ("https".equals(request.getScheme())) {
            baseUri = request.getScheme() + "://" +
                    request.getServerName() +
                    (((request.getServerPort() == 80) || (request.getServerPort() == 443)) ? "" : ":" + request.getServerPort()) +
                    "/";
        }

        return baseUri;
    }

    public static int mapError(Exception e) {
        return switch (e) {
            case BadRequestException ex -> HttpServletResponse.SC_BAD_REQUEST;
            case ModelValidationException ex -> HttpServletResponse.SC_BAD_REQUEST;
            case LinkValidationException ex -> HttpServletResponse.SC_FORBIDDEN;
            case NotFoundException ex -> HttpServletResponse.SC_NOT_FOUND;
            case UnsupportedContentTypeException ex -> HttpServletResponse.SC_NOT_ACCEPTABLE;
            case WhelkRuntimeException ex -> HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            case PostgreSQLComponent.ConflictingHoldException ex -> HttpServletResponse.SC_CONFLICT;
            case StorageCreateFailedException ex -> HttpServletResponse.SC_CONFLICT;
            case OtherStatusException ex -> ex.getCode();
            default -> HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        };
    }

    public enum DisplayMode {
        DOCUMENT, META, RAW
    }
}

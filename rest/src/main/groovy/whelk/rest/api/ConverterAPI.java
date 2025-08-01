package whelk.rest.api;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.Whelk;
import whelk.converter.marc.MarcFrameConverter;
import whelk.util.Unicode;
import whelk.util.http.HttpTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ConverterAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(ConverterAPI.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private MarcFrameConverter marcFrameConverter;

    public ConverterAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    @Override
    public void init(Whelk whelk) {
        log.info("Starting converterAPI");
        if (marcFrameConverter == null) {
            marcFrameConverter = whelk.getMarcFrameConverter();
        }
        log.info("Started ...");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        handleConversion(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        handleConversion(request, response);
    }

    private void handleConversion(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (request.getContentType() == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Missing Content-Type");
            return;
        }

        String ctype = ContentType.parse(request.getContentType()).getMimeType();
        if (!"application/ld+json".equals(ctype)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Received data in unexpected format: " + ctype);
            return;
        }

        if (request.getContentLength() == 0) {
            log.warn("Received no content to reformat.");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No content received.");
            return;
        }

        String requestedContentType = getRequestedContentType(request);

        if ("application/x-marc-json".equals(requestedContentType)) {
            try {
                String jsonText = Unicode.normalize(
                        new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                );
                Map<String, Object> json = mapper.readValue(jsonText, Map.class);
                log.info("Constructed document. Converting to {}", requestedContentType);

                Document doc = new Document(json);
                if (whelk != null) {
                    whelk.embellish(doc);
                }

                json = marcFrameConverter.runRevert(doc.data);
                String framedText = marcFrameConverter.getMapper().writeValueAsString(json);
                HttpTools.sendResponse(response, framedText, requestedContentType);
            } catch (Exception e) {
                log.error("Error during conversion", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "Error during conversion: " + e.getMessage());
            }
        } else if (requestedContentType != null) {
            String msg = "Can not convert to " + requestedContentType + ".";
            log.info(msg);
            response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, msg);
        } else {
            String msg = "No conversion requested.";
            log.info(msg);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
        }
    }

    private String getRequestedContentType(HttpServletRequest request) {
        String toParam = request.getParameter("to");
        if (toParam != null) {
            return toParam;
        }

        String acceptHeader = request.getHeader("Accept");
        if (acceptHeader != null) {
            try {
                return ContentType.parse(acceptHeader).getMimeType();
            } catch (Exception e) {
                log.debug("Could not parse Accept header: {}", acceptHeader);
            }
        }

        return "application/x-marc-json";
    }
}
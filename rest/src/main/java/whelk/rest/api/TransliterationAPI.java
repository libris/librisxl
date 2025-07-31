package whelk.rest.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.util.Romanizer;
import whelk.util.http.HttpTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static whelk.util.Jackson.mapper;

public class TransliterationAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(TransliterationAPI.class);
    private Romanizer romanizer;

    @Override
    public void init(Whelk whelk) {
        log.info("Starting Transliteration API");
        romanizer = whelk.getRomanizer();
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.debug("Handling POST request for {}", request.getPathInfo());
        
        Map<String, Object> body = getRequestBody(request);

        if (!(body.containsKey("langTag") && body.containsKey("source"))) {
            log.warn("Transliteration parameter missing");
            HttpTools.sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Parameter missing; needs langTag and source");
            return;
        }

        Object languageTag = body.get("langTag");
        Object source = body.get("source");

        if (!romanizer.isMaybeRomanizable(languageTag.toString())) {
            log.warn("Language tag {} not found", languageTag);
            HttpTools.sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid language code");
        } else {
            Map<String, String> romanized = romanizer.romanize(source.toString(), languageTag.toString());
            HttpTools.sendResponse(response, mapper.writeValueAsString(romanized), "application/json");
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.debug("Handling GET request for {}", request.getPathInfo());

        String pathInfo = request.getPathInfo();
        if (pathInfo != null && pathInfo.startsWith("/language/")) {
            handleLanguageCheck(request, response);
        } else {
            HttpTools.sendResponse(response, "", null, HttpServletResponse.SC_NOT_FOUND);
        }
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo != null && pathInfo.startsWith("/language/")) {
            handleLanguageCheck(request, response);
        }
    }

    private void handleLanguageCheck(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String pathInfo = request.getPathInfo();
        String[] parts = pathInfo.split("/", 3);
        String languageTag = parts.length > 2 ? parts[2] : "";

        if (romanizer.isMaybeRomanizable(languageTag)) {
            HttpTools.sendResponse(response, "", null, HttpServletResponse.SC_NO_CONTENT);
        } else {
            log.debug("Language tag {} not found", languageTag);
            HttpTools.sendResponse(response, "", null, HttpServletResponse.SC_NOT_FOUND);
        }
    }

    static Map<String, Object> getRequestBody(HttpServletRequest request) throws IOException {
        byte[] body = request.getInputStream().readAllBytes();

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = mapper.readValue(body, Map.class);
            return result;
        } catch (EOFException e) {
            return new HashMap<>();
        }
    }
}
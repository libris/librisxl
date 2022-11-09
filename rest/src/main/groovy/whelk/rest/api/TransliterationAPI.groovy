package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.util.Romanizer

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static whelk.util.Jackson.mapper

@Log
class TransliterationAPI extends HttpServlet {
    private static final Set<String> supportedLangTags = Romanizer.romanizableLangTags()

    @Override
    void init() {
        log.info("Starting Transliteration API")
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling POST request for ${request.pathInfo}")
        log.info("${request}")
        log.info("Request body")

        Map body = getRequestBody(request)

        if (!(body.containsKey("langTag") && body.containsKey("source"))) {
            log.warn("Transliteration parameter missing")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,  "Parameter missing; needs langTag and source")
            return
        }

        def languageTag = body["langTag"]
        def source = body["source"]
        log.info(languageTag)
        log.info(source)

        if (supportedLangTags.contains(languageTag)) {
            def romanized = Romanizer.romanize(source.toString(), languageTag.toString())
            HttpTools.sendResponse(response,  romanized, "application/json")
        } else {
            log.warn("Language tag ${languageTag} not found")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,  "Invalid language code")
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for ${request.pathInfo}")
        log.info("${request}")
        log.info("${request.getPathInfo()}")

        if (request.getPathInfo() == "/listLanguages") {
            HttpTools.sendResponse(response, ["supportedLanguages": supportedLangTags], "application/json", HttpServletResponse.SC_OK)
        } else if (request.getPathInfo().startsWith("/language/")) {
            handleLanguageCheck(request, response)
        } else {
            HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NOT_FOUND)
        }
    }

    @Override
    void doHead(HttpServletRequest request, HttpServletResponse response) {
        if (request.getPathInfo().startsWith("/language/")) {
            handleLanguageCheck(request, response)
        }
    }

    private static void handleLanguageCheck(HttpServletRequest request, HttpServletResponse response) {
        String languageTag = request.getPathInfo().split("/", 3).last()
        if (supportedLangTags.contains(languageTag)) {
            HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NO_CONTENT)
        } else {
            log.warn("Language tag ${languageTag} not found")
            HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NOT_FOUND)
        }
    }

    static Map getRequestBody(HttpServletRequest request) {
        byte[] body = request.getInputStream().getBytes()

        try {
            return mapper.readValue(body, Map)
        } catch (EOFException) {
            return [:]
        }
    }
}

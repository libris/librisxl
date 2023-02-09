package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.Whelk
import whelk.util.Romanizer
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static whelk.util.Jackson.mapper

@Log
class TransliterationAPI extends HttpServlet {

    Romanizer romanizer
    
    @Override
    void init() {
        log.info("Starting Transliteration API")
        getRomanizer()
    }
    
    private synchronized getRomanizer() {
        if (!romanizer) {
            romanizer = WhelkFactory.getSingletonWhelk().getRomanizer()
        }
        return romanizer
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling POST request for ${request.pathInfo}")

        Map body = getRequestBody(request)

        if (!(body.containsKey("langTag") && body.containsKey("source"))) {
            log.warn("Transliteration parameter missing")
            HttpTools.sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Parameter missing; needs langTag and source")
            return
        }

        def languageTag = body["langTag"]
        def source = body["source"]
        
        if (!getRomanizer().isMaybeRomanizable(languageTag)) {
            log.warn("Language tag ${languageTag} not found")
            HttpTools.sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid language code")
        } else {
            def romanized = getRomanizer().romanize(source.toString(), languageTag.toString())
            HttpTools.sendResponse(response,  romanized, "application/json")
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for ${request.pathInfo}")

        if (request.getPathInfo()?.startsWith("/language/")) {
            handleLanguageCheck(request, response)
        } else {
            HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NOT_FOUND)
        }
    }

    @Override
    void doHead(HttpServletRequest request, HttpServletResponse response) {
        if (request.getPathInfo()?.startsWith("/language/")) {
            handleLanguageCheck(request, response)
        }
    }

    private void handleLanguageCheck(HttpServletRequest request, HttpServletResponse response) {
        HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NO_CONTENT)
        
        String languageTag = request.getPathInfo().split("/", 3).last()
        if (getRomanizer().isMaybeRomanizable(languageTag)) {
            HttpTools.sendResponse(response, null, null, HttpServletResponse.SC_NO_CONTENT)
        } else {
            log.debug("Language tag ${languageTag} not found")
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

package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.util.Romanizer

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static whelk.util.Jackson.mapper

@Log
class TransliterationAPI extends HttpServlet {

    @Override
    void init() {
        log.info("Starting Transliteration API")
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for ${request.pathInfo}")
        log.info("${request}")
        log.info("Request body")


        Map body = getRequestBody(request);
        def languageTag = body["langTag"]
        def source = body["source"]
        log.info(languageTag)
        log.info(source)

        def romanized = Romanizer.romanize(source.toString(), languageTag.toString())
        HttpTools.sendResponse(response,  romanized, "application/json")
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

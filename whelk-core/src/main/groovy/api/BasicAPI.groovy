package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
abstract class BasicAPI extends BasicPlugin implements API {
    Whelk whelk

    String logMessage = "Handled #REQUESTMETHOD# for #API_ID#"

    /**
     * For discovery API.
     */
    abstract String getDescription();

    abstract protected ApiResult doHandle(Map requestMap, List pathVars)

    ApiResult handle(Map requestMap, List pathVars, String remote_ip, String method) {
        long startTime = System.currentTimeMillis()
        ApiResult result = doHandle(requestMap, pathVars)
        /*
        def headers = request.attributes.get("org.restlet.http.headers")
        def remote_ip = headers.find { it.name.equalsIgnoreCase("X-Forwarded-For") }?.value ?: request.clientInfo.address
        */
        log.info(logMessage.replaceAll("#REQUESTMETHOD#", method).replaceAll("#API_ID#", this.id) + " from $remote_ip in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
        return result
    }
}

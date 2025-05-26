package whelk.rest.api

import groovy.json.JsonException
import groovy.util.logging.Log4j2 as Log
import groovy.json.JsonSlurper
import whelk.Whelk
import whelk.util.WhelkFactory
import whelk.util.http.HttpTools
import whelk.util.http.WhelkHttpServlet

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import java.util.stream.Collectors

@Log
class UserDataAPI extends WhelkHttpServlet {
    private static final int POST_MAX_SIZE = 1000000
    static final String ID_HASH_FUNCTION = "SHA-256"

    UserDataAPI() {
    }

    UserDataAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init(Whelk whelk) {
        log.info("Starting User Data API")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        log.debug("Handling GET request for ${request.pathInfo}")

        Map userInfo = request.getAttribute("user")
        if (!isValidUserWithPermission(request, response, userInfo))
            return
        
        String id = "${userInfo.id}".digest(ID_HASH_FUNCTION)
        String data = whelk.getUserData(id) ?: upgradeOldEmailBasedEntry(userInfo) ?: "{}"

        def eTag = CrudUtils.ETag.plain(data.digest("MD5"))
        response.setHeader("ETag", eTag.toString())
        
        if (CrudUtils.getIfNoneMatch(request).map(eTag.&isNotModified).orElse(false)) {
            HttpTools.sendResponse(response, "", "application/json", HttpServletResponse.SC_NOT_MODIFIED)
        }
        else {
            HttpTools.sendResponse(response, data, "application/json")
        }
    }
    
    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        log.info("Handling PUT request for ${request.pathInfo}")

        Map userInfo = request.getAttribute("user")
        if (!isValidUserWithPermission(request, response, userInfo))
            return

        String id = "${userInfo.id}".digest(ID_HASH_FUNCTION)
        String data = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()))
        String idAndEmail = "${id} (${userInfo.id} ${userInfo.email})"

        // Arbitrary upper limit to prevent Clearly Too Large things from being saved.
        // Can't rely on request.getContentLength() because that's sent by the client.
        if (data.length() > POST_MAX_SIZE) {
            log.warn("${idAndEmail} sent too much data (length ${data.length()}, max ${POST_MAX_SIZE})")
            response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, "Too much data (length ${data.length()}, max ${POST_MAX_SIZE})")
            return
        }

        // Make sure what we're saving is actually valid JSON
        try {
            new JsonSlurper().parseText(data)
        } catch (IllegalArgumentException | JsonException e) {
            log.warn("${idAndEmail} sent invalid JSON")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,  "Invalid JSON")
            return
        }

        if (whelk.storeUserData(id, data)) {
            log.info("${idAndEmail} saved")
            response.setStatus(HttpServletResponse.SC_OK)
        } else {
            log.warn("${idAndEmail} could not be saved to database")
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Saving ${idAndEmail} to database failed")
        }
    }

    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        log.info("Handling DELETE request for ${request.pathInfo}")

        Map userInfo = request.getAttribute("user")
        if (!isValidUserWithPermission(request, response, userInfo))
            return

        whelk.removeUserData("${userInfo.id}".digest(ID_HASH_FUNCTION))
        response.setStatus(HttpServletResponse.SC_NO_CONTENT)
    }
    
    private String upgradeOldEmailBasedEntry(Map userInfo) {
        if (!userInfo.email) {
            return null
        }
        
        String email = userInfo.email.digest(ID_HASH_FUNCTION)
        String data = whelk.getUserData(email)
        if (data) {
            String id = "${userInfo.id}".digest(ID_HASH_FUNCTION)
            String idAndEmail = "${id} (${userInfo.id} ${userInfo.email})"
            if (whelk.storeUserData(id, data)) {
                whelk.removeUserData(email)
                log.info("${idAndEmail} moved from email to id")
            } else {
                log.warn("${idAndEmail} could not be saved to database")
            }
        }
        return data
    }

    private static boolean isValidUserWithPermission(HttpServletRequest request, HttpServletResponse response, Map userInfo) {
        if (!userInfo) {
            log.info("User authentication failed")
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "User authentication failed")
            return false
        }

        if (!userInfo.containsKey("id")) {
            log.info("User check failed: 'id' missing in user")
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Key 'id' missing in user info")
            return false
        }
        String id = "${userInfo.id}".digest(ID_HASH_FUNCTION)
        if (getRequestId(request) != id) {
            log.info("ID in request doesn't match ID from token")
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "ID in request doesn't match ID from token")
            return false
        }

        return true
    }

    private static String getRequestId(HttpServletRequest request) {
        return request.pathInfo == null ? "" : request.pathInfo.substring(request.pathInfo.indexOf('/') + 1)
    }
}

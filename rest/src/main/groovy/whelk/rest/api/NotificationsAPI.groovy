package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.Whelk
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Log
class NotificationsAPI extends HttpServlet {

    private Whelk whelk

    @Override
    void init() {
        log.info("Starting Notifications API")
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        Map userInfo = request.getAttribute("user")
        if (!isValidUserWithPermission(request, response, userInfo))
            return

        String id = "${userInfo.id}".digest(UserDataAPI.ID_HASH_FUNCTION)
        List<Map> data = whelk.getStorage().getNotificationsFor(id)

        HttpTools.sendResponse(response, ["notifications": data], "application/json")
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        log.info("Handling POST request for ${request.pathInfo}")

        Map userInfo = request.getAttribute("user")
        if (!isValidUserWithPermission(request, response, userInfo))
            return

        String userID = "${userInfo.id}".digest(UserDataAPI.ID_HASH_FUNCTION)
        String notificationID = request.getPathInfo().replace("/", "")
        whelk.getStorage().flipNotificationHandled(userID, Integer.parseInt(notificationID))

        response.setStatus(HttpServletResponse.SC_OK)
    }

    static boolean isValidUserWithPermission(HttpServletRequest request, HttpServletResponse response, Map userInfo) {
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

        return true
    }
}

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
        log.info("Starting User Data API")
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        Map userInfo = request.getAttribute("user")
        if (!UserDataAPI.isValidUserWithPermission(request, response, userInfo))
            return

        String id = "${userInfo.id}".digest(UserDataAPI.ID_HASH_FUNCTION)
        List<Map> data = whelk.getStorage().getNotificationsFor(id)

        HttpTools.sendResponse(response, ["THE LIST": data], "application/json")
    }
}

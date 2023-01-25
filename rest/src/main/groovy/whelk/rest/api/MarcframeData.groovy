package whelk.rest.api

import groovy.transform.CompileStatic
import whelk.converter.marc.MarcFrameConverter

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static whelk.rest.api.HttpTools.sendResponse

@CompileStatic
class MarcframeData extends HttpServlet {

    String cachedResponseBody

    @Override
    void init() {
        synchronized (this) {
            if (cachedResponseBody == null) {
                var mfconverter = new MarcFrameConverter()
                Map data = (Map) mfconverter.readConfig('marcframe.json')
                cachedResponseBody = mfconverter.mapper.writeValueAsString(data)
            }
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        sendResponse(response, cachedResponseBody, "application/json")
    }

}

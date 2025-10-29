package whelk.rest.api;

import whelk.converter.marc.MarcFrameConverter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

import static whelk.util.http.HttpTools.sendResponse;

public class MarcframeData extends HttpServlet {

    private String cachedResponseBody;

    @Override
    public void init() throws ServletException {
        synchronized (this) {
            if (cachedResponseBody == null) {
                try {
                    var mfconverter = new MarcFrameConverter();
                    Map<?, ?> data = (Map<?, ?>) mfconverter.readConfig("marcframe.json");
                    cachedResponseBody = mfconverter.getMapper().writeValueAsString(data);
                } catch (IOException e) {
                    throw new ServletException("Failed to initialize MarcframeData", e);
                }
            }
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        sendResponse(response, cachedResponseBody, "application/json");
    }

}

package whelk.util.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.util.WhelkFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class WhelkHttpServlet extends HttpServlet {
    private final static Logger log = LogManager.getLogger(WhelkHttpServlet.class);

    protected volatile Whelk whelk;
    private final Object whelkInitLock = new Object();

    /**
     * Override this to do additional initialization after Whelk has been created
     *
     * @param whelk
     */
    protected void init(Whelk whelk) {

    }

    protected void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        if (whelk == null) {
            try {
                initWhelk();
            } catch (Exception e) {
                log.error("Failed to initialize Whelk", e);
                HttpTools.sendError(res, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Whelk unavailable");
                return;

            }

        }

        super.service(req, res);
    }

    public void init() {
        try {
            initWhelk();
        } catch (Exception e) {
            log.error("Failed to initialize Whelk", e);
        }

    }

    private void initWhelk() throws Exception {
        if (whelk == null) {
            synchronized (whelkInitLock) {
                if (whelk == null) {
                    whelk = createWhelk();
                    init(whelk);
                }

            }

        }

    }

    protected Whelk createWhelk() {
        return WhelkFactory.getSingletonWhelk();
    }


}


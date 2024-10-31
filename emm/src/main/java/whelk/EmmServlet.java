package whelk;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class EmmServlet extends HttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final HashSet<String> availableCategories;
    private final Whelk whelk;
    public EmmServlet() {
        availableCategories = new HashSet<>();
        availableCategories.add("all");
        whelk = Whelk.createLoadedCoreWhelk();
    }

    public void init() {
    }

    public void destroy() {
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {
            String category = req.getParameter("category");
            String until = req.getParameter("until");

            System.err.println("category: " + category);
            System.err.println("until: " + until);

            if (!availableCategories.contains(category)) {
                res.sendError(400); // temp
                return;
            }

            if (until == null) { // Send EntryPoint reply
                res.sendError(400); // temp
                return;
            }

            String ApiBaseUrl = req.getRequestURL().toString();

            // Send ChangeSet reply
            res.setCharacterEncoding("utf-8");
            res.setContentType("application/json");
            EmmChangeSet.sendChangeSet(whelk, res, category, until, ApiBaseUrl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

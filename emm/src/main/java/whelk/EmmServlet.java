package whelk;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static whelk.util.Jackson.mapper;

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
            String ApiBaseUrl = req.getRequestURL().toString();

            res.setCharacterEncoding("utf-8");
            res.setContentType("application/json");

            if (!availableCategories.contains(category)) {
                res.sendError(404);
                return;
            }

            // Send an Entry-Point reply
            if (until == null) {
                HashMap responseObject = new HashMap();
                ArrayList contexts = new ArrayList();
                contexts.add("https://www.w3.org/ns/activitystreams");
                contexts.add("https://emm-spec.org/1.0/context.json");
                responseObject.put("@context", contexts);
                responseObject.put("type", "OrderedCollection");
                responseObject.put("id", ApiBaseUrl+"?category="+category);

                HashMap first = new HashMap();
                first.put("type", "OrderedCollectionPage");
                first.put("id", ApiBaseUrl+"?category="+category+"&until="+System.currentTimeMillis());

                responseObject.put("first", first);


                String jsonResponse = mapper.writeValueAsString(responseObject);
                BufferedWriter writer = new BufferedWriter( res.getWriter() );
                writer.write(jsonResponse);
                writer.close();
                return;
            }

            // Send ChangeSet reply
            EmmChangeSet.sendChangeSet(whelk, res, category, until, ApiBaseUrl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

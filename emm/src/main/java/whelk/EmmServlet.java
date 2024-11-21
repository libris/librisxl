package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import static whelk.util.Jackson.mapper;

public class EmmServlet extends HttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Whelk whelk;
    public EmmServlet() {
        whelk = Whelk.createLoadedCoreWhelk();
    }

    public void init() {
    }

    public void destroy() {
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) {
        try {
            String dump = req.getParameter("dump");
            String until = req.getParameter("until");
            String apiBaseUrl = req.getRequestURL().toString();

            res.setCharacterEncoding("utf-8");
            res.setContentType("application/activity+json");

            if (dump != null) {
                Dump.sendDumpResponse(whelk, apiBaseUrl, req, res);
                return;
            }

            // Send an Entry-Point reply
            if (until == null) {
                var responseObject = new LinkedHashMap<>();
                var contexts = new ArrayList<>();
                contexts.add("https://www.w3.org/ns/activitystreams");
                contexts.add("https://emm-spec.org/1.0/context.json");
                responseObject.put("@context", contexts);
                responseObject.put("type", "OrderedCollection");
                responseObject.put("id", apiBaseUrl);
                responseObject.put("url", apiBaseUrl+"?dump=index");
                var first = new LinkedHashMap<>();
                first.put("type", "OrderedCollectionPage");
                first.put("id", apiBaseUrl+"?until="+System.currentTimeMillis());
                responseObject.put("first", first);

                String jsonResponse = mapper.writeValueAsString(responseObject);
                BufferedWriter writer = new BufferedWriter( res.getWriter() );
                writer.write(jsonResponse);
                writer.close();
                return;
            }

            // Send ChangeSet reply
            EmmChangeSet.sendChangeSet(whelk, res, until, apiBaseUrl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

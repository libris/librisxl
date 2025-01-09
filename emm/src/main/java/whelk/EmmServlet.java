package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.util.http.HttpTools;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class EmmServlet extends HttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Whelk whelk;
    private final TargetVocabMapper targetVocabMapper;

    public static final String AS2_CONTENT_TYPE = "application/activity+json";

    public EmmServlet() {
        whelk = Whelk.createLoadedCoreWhelk();
        Document contextDocument = whelk.getStorage().getDocumentByIri(whelk.getSystemContextUri());
        targetVocabMapper = new TargetVocabMapper(whelk.getJsonld(), contextDocument.data);
    }

    public void init() {
    }

    public void destroy() {
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) {
        try {
            String apiBaseUrl = req.getRequestURL().toString();

            if (req.getServletPath() != null && req.getServletPath().endsWith("/full")) {
                Dump.sendDumpResponse(whelk, targetVocabMapper, apiBaseUrl, req, res);
                return;
            }
            String until = req.getParameter("until");

            // Send an Entry-Point reply
            if (until == null) {
                var responseObject = new LinkedHashMap<>();
                var contexts = new ArrayList<>();
                contexts.add("https://www.w3.org/ns/activitystreams");
                contexts.add("https://emm-spec.org/1.0/context.json");
                responseObject.put("@context", contexts);
                responseObject.put("type", "OrderedCollection");
                responseObject.put("id", apiBaseUrl);
                responseObject.put("url", apiBaseUrl + (apiBaseUrl.endsWith("/") ? "full" : "/full"));
                var first = new LinkedHashMap<>();
                first.put("type", "OrderedCollectionPage");
                first.put("id", apiBaseUrl+"?until="+System.currentTimeMillis());
                responseObject.put("first", first);

                HttpTools.sendResponse(res, responseObject, AS2_CONTENT_TYPE);
                return;
            }

            // Send ChangeSet reply
            EmmChangeSet.sendChangeSet(whelk, res, until, apiBaseUrl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

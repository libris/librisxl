package whelk.rest.api;

import whelk.Document;
import whelk.Whelk;
import whelk.util.LegacyIntegrationTools;
import whelk.util.http.WhelkHttpServlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static whelk.util.Jackson.mapper;

public class HoldAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(HoldAPI.class);

    public HoldAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    public HoldAPI(Whelk whelk) {
        this.whelk = whelk;
        init(whelk);
    }

    @Override
    public void init(Whelk whelk) {
        log.info("Starting HoldAPI");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String library = request.getParameter("library");
        String id = request.getParameter("id");
        if (id == null || library == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"library\" (sigel) and \"id\" parameters required.");
            return;
        }
        id = LegacyIntegrationTools.fixUri(id);
        library = LegacyIntegrationTools.fixUri(library);
        String recordId = whelk.getStorage().getRecordId(id);
        if (recordId == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.");
            return;
        }

        Document document = whelk.getStorage().loadDocumentByMainId(recordId);
        String collection = LegacyIntegrationTools.determineLegacyCollection(document, whelk.getJsonld());
        if (!"bib".equals(collection)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.");
            return;
        }

        library = LegacyIntegrationTools.legacySigelToUri(library);
        if (library == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Could not find a profile for the supplied \"library\"-parameter.");
            return;
        }

        List<Document> holdings = whelk.getAttachedHoldings(document.getThingIdentifiers());
        List<String> holdingIDs = new ArrayList<>();
        for (Document holding : holdings) {
            if (holding.getHeldBy().equals(library)) {
                holdingIDs.add(holding.getCompleteId());
            }
        }

        String jsonString = mapper.writeValueAsString(holdingIDs);
        response.setContentType("application/json");
        response.setHeader("Expires", "0");
        response.setHeader("Cache-Control", "no-cache");
        OutputStream out = response.getOutputStream();
        out.write(jsonString.getBytes(StandardCharsets.UTF_8));
        out.close();
    }
}

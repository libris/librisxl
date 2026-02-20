package whelk.rest.api;

import com.fasterxml.jackson.core.JsonParseException;
import whelk.Document;
import whelk.Whelk;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static whelk.FeatureFlags.Flag.INDEX_BLANK_WORKS;
import static whelk.util.Jackson.mapper;

/**
 * The purpose of this class is to facilitate refreshing of records that have been modified in authoritative storage
 * (postgresql) by some non-standard measure (for example by hand). A POST request is made to this class with a ?mode=[loud|quiet]
 * parameter and a json list of the IDs (or sameAs-IDs) that are to be refreshed as request body.
 *
 * Refreshing a record in this context means updating all derivative data of that record in the various places where
 * such data is stored. For example: id/sameAs-tables, dependency-tables, ElasticSearch etc.
 */
public class RefreshAPI extends WhelkHttpServlet {

    public RefreshAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    public RefreshAPI(Whelk whelk) {
        this.whelk = whelk;
        init(whelk);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

        Boolean loudMode = parseLoudMode(request.getQueryString());
        if (loudMode == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "You need to specify the \"mode\" parameter as either \"loud\" or \"quiet\". \"Loud\" in this " +
                            "context means modified-timestamps for the refreshed records are to be updated. " +
                            "This in turn will result in all touched documents being pushed out (again) through the " +
                            "various export channels.");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Cache-Control", "no-cache");
        OutputStream out = response.getOutputStream();

        BufferedReader reader = request.getReader();
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }

        List<String> ids;
        try {
            ids = mapper.readValue(builder.toString(), ArrayList.class);
        } catch (JsonParseException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed json body: " + e);
            return;
        }

        long count = 0;
        for (String id : ids) {
            Document document = whelk.getStorage().load(id);
            if (document != null) {
                if (loudMode) {
                    refreshLoudly(document);
                } else {
                    refreshQuietly(document);
                }
                ++count;
            } else {
                out.write((id + " - Failed to load\n").getBytes());
            }
        }

        if (loudMode) {
            out.write(("Refreshed " + count + " records (loudly).\n").getBytes());
        } else {
            out.write(("Refreshed " + count + " records (quietly).\n").getBytes());
        }
        out.close();
    }

    Boolean parseLoudMode(String queryString) {
        if (queryString.equals("mode=loud")) {
            return true;
        }
        if (queryString.equals("mode=quiet")) {
            return false;
        }
        return null;
    }

    void refreshLoudly(Document doc) {
        boolean minorUpdate = false;
        whelk.storeAtomicUpdate(doc.getShortId(), minorUpdate, true, false, "xl", "Libris admin",
                (_doc) -> {
                    _doc.data = doc.data;
                });
    }

    void refreshQuietly(Document doc) {
        whelk.getStorage().refreshDerivativeTables(doc, false);
        whelk.elastic.index(doc, whelk);
        if (whelk.getFeatures().isEnabled(INDEX_BLANK_WORKS)) {
            for (var id : doc.getVirtualRecordIds()) {
                whelk.elastic.index(doc.getVirtualRecord(id), whelk);
            }
        }
    }
}

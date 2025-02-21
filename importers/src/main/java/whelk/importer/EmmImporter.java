package whelk.importer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.util.LegacyIntegrationTools;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.util.Jackson.mapper;

public class EmmImporter {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Whelk whelk;
    private final boolean usingQuickCreate;
    final String emmBaseUrl;

    public EmmImporter(Whelk whelk, String emmBaseUrl, boolean usingQuickCreate) {
        this.whelk = whelk;
        this.emmBaseUrl = emmBaseUrl;
        this.usingQuickCreate = usingQuickCreate;
    }

    public void importFromLibrisEmm(String[] types) throws URISyntaxException, IOException, InterruptedException {
        try (HttpClient client = HttpClient.newHttpClient()) {
            for (String type : types) {
                while (!importStream(client, type)) {
                    logger.info("Restarting EMM import of " + type + " from " + emmBaseUrl + " because the dump date shifted while downloading.");
                }
            }
        }
        if (usingQuickCreate) {
            logger.info("Since --quick-create was used, dependency tables and so on must/will now be refreshed. Don't forget to also reindex!");
            whelk.getStorage().reDenormalize();
        }
    }

    private boolean importStream(HttpClient client, String type) throws URISyntaxException, IOException, InterruptedException {
        URI next = new URI(emmBaseUrl).resolve("/full?selection=type:" + type + "&offset=0");
        String dumpTimeStamp = null;
        int importCount = 0;
        int percentProgress = 0;

        while (next != null) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(next)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map responseMap = mapper.readValue(response.body(), Map.class);
            if (responseMap.containsKey("next")) {
                next = new URI( (String) responseMap.get("next") );
            } else {
                next = null;
            }

            // Check that the dump hasn't been switched under our feet
            if (dumpTimeStamp == null) {
                if (responseMap.containsKey("startTime")) {
                    dumpTimeStamp = (String) responseMap.get("startTime");
                }
            }
            if (responseMap.containsKey("startTime") && !responseMap.get("startTime").equals(dumpTimeStamp)) {
                return false;
            }

            // Check progress
            if (responseMap.containsKey("totalItems") && responseMap.get("totalItems") instanceof Integer) {
                int totalItems = (int) responseMap.get("totalItems");
                int percent = (int) ( 100.0 * (double)importCount / (double) totalItems );
                if ( percent > percentProgress) {
                    percentProgress = percent;
                    System.err.print("At " + percentProgress + "% (" + importCount + ") of emm import of type " + type + " (and subtypes) from " + emmBaseUrl + "\r");
                }
            }

            // Create records
            if (responseMap.containsKey("items")) {
                List items = (List) responseMap.get("items");
                for (Object item : items) {
                    Map itemMap = (Map) item;
                    if (itemMap.containsKey("@graph")) {
                        HashMap docMap = new HashMap();
                        docMap.put("@graph", itemMap.get("@graph")); // We just want the graph list, not the other attached stuff
                        Document doc = new Document(docMap);
                        claimRecord(doc);
                        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, whelk.getJsonld());
                        if (usingQuickCreate) {
                            whelk.quickCreateDocument(doc, "EMM-importer", emmBaseUrl, collection);
                        } else {
                            whelk.createDocument(doc, "EMM-importer", emmBaseUrl, collection, false);
                        }
                        ++importCount;
                    }
                }
            }
        }

        return true;
    }

    private static void claimRecord(Document doc) {
        String oldId = doc.getId();
        String idKernel = oldId.substring(oldId.lastIndexOf('/')+1);
        String newId = Document.getBASE_URI().resolve(idKernel).toString();
        doc.deepReplaceId(newId);
    }

}

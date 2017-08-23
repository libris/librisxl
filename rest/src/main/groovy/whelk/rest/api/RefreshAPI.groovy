package whelk.rest.api

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader
import whelk.util.LegacyIntegrationTools

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * The purpose of this class is to facilitate refreshing of records that have been modified in authoritative storage
 * (postgresql) by some non-standard measure (for example by hand). A GET request is made to this class with a ?mode=[loud|quiet]
 * parameter and a linebreak-separated list of the IDs (or sameAs-IDs) that are to be refreshed as request body.
 *
 * Refreshing a record in this context means updating all derivative data of that record in the various places where
 * such data is stored. For example: id/sameAs-tables, dependency-tables, ElasticSearch etc.
 */
class RefreshAPI extends HttpServlet
{
    private Whelk whelk
    private JsonLd jsonld

    public RefreshAPI() {
        Properties secretProperties = PropertyLoader.loadProperties("secret")
        PostgreSQLComponent postgreSqlComponent = new PostgreSQLComponent(
                secretProperties.getProperty("sqlUrl"),
                secretProperties.getProperty("sqlMaintable"))
        ElasticSearch elasticSearch = new ElasticSearch(
                (String) secretProperties.get("elasticHost"),
                (String) secretProperties.get("elasticCluster"),
                (String) secretProperties.get("elasticIndex"))

        whelk = new Whelk(postgreSqlComponent, elasticSearch)
        whelk.loadCoreData()
        jsonld = new JsonLd(whelk.getDisplayData(), whelk.getVocabData())
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {

        boolean loudMode
        String mode = request.getParameter("mode")
        switch (mode) {
            case "loud":
                loudMode = true
                break
            case "quiet":
                loudMode = false
                break
            default:
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "You need to specify the \"mode\" parameter as either \"loud\" or \"quiet\". \"Loud\" in this " +
                                "context means modified-timestamps for the refreshed records are to be updated. " +
                                "This in turn will result in all touched documents being pushed out (again) through the " +
                                "various export channels.")
                return
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()))
        response.setStatus(HttpServletResponse.SC_OK)
        OutputStream out = response.getOutputStream()

        long count = 0
        String line
        while ( (line = reader.readLine()) != null ) {
            String recordId = whelk.storage.getRecordId(line)
            if (recordId != null) {
                Document document = whelk.storage.loadDocumentByMainId(recordId)
                if (document != null) {
                    if (loudMode)
                        refreshLoudly(document)
                    else
                        refreshQuietly(document)
                } else {
                    out.println(recordId + " - Failed to load (received as: " + line + ")")
                }
            } else {
                out.println(line + " - No such ID/sameAs")
            }
            ++count
        }

        if (loudMode)
            out.println("Refreshed " + count + " records (loudly).")
        else
            out.println("Refreshed " + count + " records (quietly).")
        out.close()
    }

    void refreshLoudly(Document doc) {
        boolean minorUpdate = false
        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
        whelk.storeAtomicUpdate(doc.getShortId(), minorUpdate, "xl", "Libris admin", collection, doc.deleted, {
            Document _doc ->
                _doc.data = doc.data
        })
    }

    void refreshQuietly(Document doc) {
        whelk.storage.refreshDerivativeTables(doc)
        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
        whelk.elastic.index(doc, collection, whelk)
        whelk.reindexDependers(doc)
    }
}

package whelk.rest.api

import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.Whelk
import whelk.util.LegacyIntegrationTools
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * The purpose of this class is to facilitate refreshing of records that have been modified in authoritative storage
 * (postgresql) by some non-standard measure (for example by hand). A POST request is made to this class with a ?mode=[loud|quiet]
 * parameter and a json list of the IDs (or sameAs-IDs) that are to be refreshed as request body.
 *
 * Refreshing a record in this context means updating all derivative data of that record in the various places where
 * such data is stored. For example: id/sameAs-tables, dependency-tables, ElasticSearch etc.
 */
class RefreshAPI extends HttpServlet
{
    public final static mapper = new ObjectMapper()
    private Whelk whelk

    RefreshAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    RefreshAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {

        Boolean loudMode = parseLoudMode(request.getQueryString())
        if (loudMode == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "You need to specify the \"mode\" parameter as either \"loud\" or \"quiet\". \"Loud\" in this " +
                            "context means modified-timestamps for the refreshed records are to be updated. " +
                            "This in turn will result in all touched documents being pushed out (again) through the " +
                            "various export channels.")
            return
        }

        response.setStatus(HttpServletResponse.SC_OK)
        response.setHeader('Cache-Control', 'no-cache')
        OutputStream out = response.getOutputStream()

        Reader reader = request.getReader()
        StringBuilder builder = new StringBuilder()
        String line
        while( (line = reader.readLine()) != null )
            builder.append(line)

        List ids
        try {
            ids = mapper.readValue(builder.toString(), ArrayList)
        } catch (JsonParseException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Malformed json body: " + e)
            return
        }

        long count = 0
        for (String id : ids) {
            Document document = whelk.storage.load(id)
            if (document != null) {
                if (loudMode)
                    refreshLoudly(document)
                else
                    refreshQuietly(document)
                ++count
            } else {
                out.println(id + " - Failed to load")
            }
        }

        if (loudMode)
            out.println("Refreshed " + count + " records (loudly).")
        else
            out.println("Refreshed " + count + " records (quietly).")
        out.close()
    }

    Boolean parseLoudMode(String queryString) {
        if (queryString.equals("mode=loud"))
            return true
        if (queryString.equals("mode=quiet"))
            return false
        return null
    }

    void refreshLoudly(Document doc) {
        boolean minorUpdate = false
        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, whelk.getJsonld())
        whelk.storeAtomicUpdate(doc.getShortId(), minorUpdate, "xl", "Libris admin", {
            Document _doc ->
                _doc.data = doc.data
        })
    }

    void refreshQuietly(Document doc) {
        whelk.storage.refreshDerivativeTables(doc)
        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, whelk.getJsonld())
        whelk.elastic.index(doc, collection, whelk)
        whelk.reindexDependers(doc)
    }
}

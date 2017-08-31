package whelk.rest.api

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.PropertyLoader
import whelk.util.LegacyIntegrationTools

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class HoldAPI extends HttpServlet {

    private Whelk whelk
    private JsonLd jsonld
    private JsonLD2MarcXMLConverter toMarcXmlConverter

    @Override
    void init() {
        Properties configuration = PropertyLoader.loadProperties("secret")
        PostgreSQLComponent storage = new PostgreSQLComponent(configuration.getProperty("sqlUrl"),
                configuration.getProperty("sqlMaintable"))
        whelk = new Whelk(storage)
        whelk.loadCoreData()
        jsonld = new JsonLd(whelk.displayData, whelk.vocabData)
        toMarcXmlConverter = new JsonLD2MarcXMLConverter()
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        String library = request.getParameter("library")
        String id = request.getParameter("id")
        if (id == null || library == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"library\" (sigel) and \"id\" parameters required.")
            return
        }
        id = LegacyIntegrationTools.fixUri(id)
        library = LegacyIntegrationTools.fixUri(library)
        String recordId = whelk.storage.getRecordId(id)
        if (recordId == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }

        Document document = whelk.storage.loadDocumentByMainId(recordId)
        String collection = whelk.util.LegacyIntegrationTools.determineLegacyCollection(document, jsonld)
        if (!collection.equals("bib")){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }

        library = whelk.util.LegacyIntegrationTools.legacySigelToUri(library)
        if (library == null){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Could not find a profile for the supplied \"library\"-parameter.")
            return
        }

        List<Document> holdings = whelk.storage.getAttachedHoldings(document.getThingIdentifiers())
        for(Document holding : holdings) {
            if (holding.getHeldBy().equals(library)) {
                response.sendRedirect(holding.getCompleteId())
                return
            }
        }

        response.sendError(HttpServletResponse.SC_NOT_FOUND)
    }
}

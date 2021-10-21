package whelk.rest.api

import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.Jackson
import whelk.util.LegacyIntegrationTools
import whelk.util.WhelkFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static whelk.util.Jackson.mapper

class HoldAPI extends HttpServlet {

    private Whelk whelk
    private JsonLD2MarcXMLConverter toMarcXmlConverter

    HoldAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    HoldAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = WhelkFactory.getSingletonWhelk()
        }
        toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter())
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
        String collection = LegacyIntegrationTools.determineLegacyCollection(document, whelk.jsonld)
        if (collection != "bib"){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }

        library = LegacyIntegrationTools.legacySigelToUri(library)
        if (library == null){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Could not find a profile for the supplied \"library\"-parameter.")
            return
        }

        List<Document> holdings = whelk.getAttachedHoldings(document.getThingIdentifiers())
        List<String> holdingIDs = []
        for (Document holding in holdings) {
            if (holding.getHeldBy().equals(library))
                holdingIDs.add(holding.getCompleteId())
        }

        String jsonString = mapper.writeValueAsString(holdingIDs)
        response.setContentType("application/json")
        response.setHeader("Expires", "0")
        response.setHeader('Cache-Control', 'no-cache')
        OutputStream out = response.getOutputStream()
        out.write(jsonString.getBytes("UTF-8"))
        out.close()
    }
}

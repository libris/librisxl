package whelk.rest.api

import groovy.xml.XmlUtil
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * An API for fetching a legacy "complete" virtual MARC21 record for a known ID.
 *
 * The purpose of this API is to fill the vacuum left by "librisXP".
 */
class LegacyMarcAPI extends HttpServlet {

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

    /**
     * Request entry point
     */
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        String library = request.getParameter("library")
        String id = request.getParameter("id")
        if (id == null || library == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"library\" (sigel) and \"id\" parameters required.")
            return
        }
        Document rootDocument = getDocument(id)
        if (rootDocument == null){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }
        String collection = LegacyIntegrationTools.determineLegacyCollection(rootDocument, jsonld)
        if (!collection.equals("bib")){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }

        library = LegacyIntegrationTools.legacySigelToUri(library)

        response.getOutputStream().println("Wiring ok.")
    }

    /**
     * Get a document, on any valid ID/sameas for said document
     */
    Document getDocument(String idOrSameAs) {
        String recordId = whelk.storage.getRecordId(idOrSameAs)
        if (recordId == null)
            return null
        return whelk.storage.load(recordId)
    }

    /**
     * Make a marc xml string out of a whelk document
     */
    String toXmlString(Document doc) {
        try {
            return (String) toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.getNON_JSON_CONTENT_KEY())
        }
        catch (Exception | Error e) { // Depending on the converter, a variety of problems may arise here{
            return null
        }
    }

    /**
     * Make a marc xml string out of a XmlSlurper node
     */
    String toXmlString(node) {
        XmlUtil.serialize(node)
    }

    void compileVirtualMarcRecord(String bibXmlString) {
        MarcRecord marcRecord = MarcXmlRecordReader.fromXml(bibXmlString)

    }
}

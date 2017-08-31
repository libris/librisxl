package whelk.rest.api

import groovy.xml.XmlUtil
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import se.kb.libris.export.ExportProfile
import se.kb.libris.export.MergeRecords

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
        id = LegacyIntegrationTools.fixUri(id)
        library = LegacyIntegrationTools.fixUri(library)
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
        String profileString = whelk.storage.getProfileByLibraryUri(library)
        if (profileString == null){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Could not find a profile for the supplied \"library\"-parameter.")
            return
        }

        File tempFile = File.createTempFile("profile", ".tmp")
        tempFile.write(profileString)
        ExportProfile profile = new ExportProfile(tempFile)
        tempFile.delete()

        Vector<MarcRecord> result = compileVirtualMarcRecord(profile, rootDocument)

        response.setContentType("application/octet-stream")
        Iso2709MarcRecordWriter writer = new Iso2709MarcRecordWriter(response.getOutputStream(), "UTF-8")
        for (MarcRecord record : result) {
            writer.writeRecord(record)
        }
        response.getOutputStream().close()
    }

    Vector<MarcRecord> compileVirtualMarcRecord(ExportProfile profile, Document rootDocument) {
        String bibXmlString = toXmlString(rootDocument)
        def xmlRecord = new XmlSlurper(false, false).parseText(bibXmlString)

        List auth_ids = []
        xmlRecord.datafield.subfield.each {
            if (it.@code.text().equals("0") && (it.text().startsWith("https://id.kb.se/") || it.text().startsWith("https://libris.kb.se/"))) {
                auth_ids.add(it.text())
            }
        }

        def auths = new HashSet<MarcRecord>()
        if (!profile.getProperty("authtype", "NONE").equalsIgnoreCase("NONE")) {
            auth_ids.each { String auth_id ->
                auths.add(MarcXmlRecordReader.fromXml(toXmlString(getDocument(auth_id))))
            }
        }

        List<Document> holdingDocuments = whelk.storage.getAttachedHoldings(rootDocument.getThingIdentifiers())
        def holdings = new TreeMap<String, MarcRecord>()

        if (!profile.getProperty("holdtype", "NONE").equalsIgnoreCase("NONE")) {
            for (Document holding : holdingDocuments) {
                holdings.put(holding.getSigel(), MarcXmlRecordReader.fromXml(toXmlString(holding)))
            }
        }

        return profile.mergeRecord(MarcXmlRecordReader.fromXml(bibXmlString), holdings, auths)
    }

    /**
     * Get a document, on any valid ID/sameas for said document
     */
    Document getDocument(String idOrSameAs) {
        String recordId = whelk.storage.getRecordId(idOrSameAs)
        if (recordId == null)
            return null
        return whelk.storage.loadDocumentByMainId(recordId)
    }

    /**
     * Make a marc xml string out of a whelk document
     */
    String toXmlString(Document doc) {
        try {
            return (String) toMarcXmlConverter.convert(doc.data, doc.getShortId()).get(JsonLd.getNON_JSON_CONTENT_KEY())
        }
        catch (Exception | Error e) { // Depending on the converter, a variety of problems may arise here
            return null
        }
    }
}

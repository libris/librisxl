package whelk.rest.api

import groovy.util.logging.Log4j2
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
@Log4j2
class LegacyMarcAPI extends HttpServlet {

    private Whelk whelk
    private JsonLD2MarcXMLConverter toMarcXmlConverter

    private static final defaultProfileString =
            "month=*\n" +
            "move240to244=on\n" +
            "f003=SE-LIBR\n" +
            "year=*\n" +
            "holdupdate=on\n" +
            "lcsh=off\n" +
            "licensefilter=off\n" +
            "composestrategy=compose\n" +
            "holddelete=on\n" +
            "authtype=interleaved\n" +
            "isbnhyphenate=off\n" +
            "name=SEK\n" +
            "contact=support@libris.kb.se\n" +
            "delivery_type=LIBRISFTP\n" +
            "day_in_month=*\n" +
            "locations=SEK FREE Nyks\n" +
            "bibcreate=off\n" +
            "period=1\n" +
            "authcreate=off\n" +
            "format=ISO2709\n" +
            "status=on\n" +
            "longname=LIBRIS testprofil\n" +
            "extrafields=Mtm\\:852,856\n" +
            "biblevel=off\n" +
            "issnhyphenate=off\n" +
            "issndehyphenate=off\n" +
            "errcontact=support@libris.kb.se\n" +
            "holdtype=interleaved\n" +
            "holdcreate=on\n" +
            "isbndehyphenate=on\n" +
            "characterencoding=Latin1Strip\n" +
            "bibupdate=off\n" +
            "day_in_week=*\n" +
            "efilter=off\n" +
            "biboperators=\n" +
            "move0359=off\n" +
            "authupdate=off\n" +
            "sab=off"

    LegacyMarcAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    LegacyMarcAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = Whelk.createLoadedCoreWhelk()
        }
        toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.createMarcFrameConverter())
    }

    /**
     * Request entry point
     */
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            String library = request.getParameter("library")
            String id = request.getParameter("id")
            if (id == null || library == null) {
                String message = "\"library\" (sigel) and \"id\" parameters required."
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message)
                log.warn("Bad client request to LegacyMarcAPI: " + message)
                return
            }
            id = LegacyIntegrationTools.fixUri(id)
            library = LegacyIntegrationTools.fixUri(library)
            Document rootDocument = getDocument(id)
            if (rootDocument == null) {
                String message = "The supplied \"id\"-parameter must refer to an existing bibliographic record."
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message)
                log.warn("Bad client request to LegacyMarcAPI: " + message)
                return
            }
            String collection = LegacyIntegrationTools.determineLegacyCollection(rootDocument, whelk.jsonld)
            if (!collection.equals("bib")) {
                String message = "The supplied \"id\"-parameter must refer to an existing bibliographic record."
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message)
                log.warn("Bad client request to LegacyMarcAPI: " + message)
                return
            }
            String profileString = whelk.storage.getProfileByLibraryUri(library)
            if (profileString == null) {
                String message = "Could not find a profile for the supplied \"library\"-parameter:" + library + ", using default profile."
                log.warn("Bad client request to LegacyMarcAPI: " + message)
                profileString = defaultProfileString
            }

            File tempFile = File.createTempFile("profile", ".tmp")
            tempFile.write(profileString)
            ExportProfile profile = new ExportProfile(tempFile)
            tempFile.delete()

            // Embellish data
            whelk.embellish(rootDocument, false)

            Vector<MarcRecord> result = compileVirtualMarcRecord(profile, rootDocument)

            response.setContentType("application/octet-stream")
            response.setHeader("Content-Disposition", "attachment; filename=\"libris_marc_" + rootDocument.getShortId() + "\"")
            Iso2709MarcRecordWriter writer = new Iso2709MarcRecordWriter(response.getOutputStream(), "UTF-8")
            for (MarcRecord record : result) {
                writer.writeRecord(record)
            }
            response.getOutputStream().close()
        } catch (Throwable e) {
            log("Failed handling _compilemarc request: ", e)
            throw e
        }
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
                Document authDoc = getDocument(auth_id)
                if (authDoc != null) {
                    String xmlString = toXmlString(authDoc)
                    if (xmlString != null)
                        auths.add(MarcXmlRecordReader.fromXml(xmlString))
                }
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

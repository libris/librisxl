package whelk.rest.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.Iso2709MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcRecordWriter;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.MarcExport;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

/**
 * An API for fetching a legacy "complete" virtual MARC21 record for a known ID.
 *
 * The purpose of this API is to fill the vacuum left by "librisXP".
 */
public class LegacyMarcAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(LegacyMarcAPI.class);

    private JsonLD2MarcXMLConverter toMarcXmlConverter;

    private static final String defaultProfileString =
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
            "sab=off";

    public LegacyMarcAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    public LegacyMarcAPI(Whelk whelk) {
        this.whelk = whelk;
        init(whelk);
    }

    @Override
    protected void init(Whelk whelk) {
        toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
    }

    /**
     * Request entry point
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            response.setCharacterEncoding("UTF-8");

            String library = request.getParameter("library");
            String id = request.getParameter("id");
            if (id == null || library == null) {
                String message = "\"library\" (sigel) and \"id\" parameters required.";
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
                log.warn("Bad client request to LegacyMarcAPI: " + message);
                return;
            }
            id = LegacyIntegrationTools.fixUri(id);
            library = LegacyIntegrationTools.fixUri(library);
            String systemId = whelk.getStorage().getSystemIdByIri(id);
            if (systemId == null) {
                String message = "The supplied \"id\"-parameter must refer to an existing bibliographic record.";
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
                log.warn("Bad client request to LegacyMarcAPI: " + message);
                return;
            }
            Document rootDocument = whelk.loadEmbellished(systemId);
            if (rootDocument == null) {
                String message = "The supplied \"id\"-parameter must refer to an existing bibliographic record.";
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
                log.warn("Bad client request to LegacyMarcAPI: " + message);
                return;
            }
            String collection = LegacyIntegrationTools.determineLegacyCollection(rootDocument, whelk.jsonld);
            if (!collection.equals("bib")) {
                String message = "The supplied \"id\"-parameter must refer to an existing bibliographic record.";
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, message);
                log.warn("Bad client request to LegacyMarcAPI: " + message);
                return;
            }
            String profileString = whelk.getStorage().getProfileByLibraryUri(library);
            if (profileString == null) {
                String message = "Could not find a profile for the supplied \"library\"-parameter:" + library + ", using default profile.";
                log.warn("Bad client request to LegacyMarcAPI: " + message);
                profileString = defaultProfileString;

                // This is a hack, to allow holding-information to be included when using the default profile
                String sigel = LegacyIntegrationTools.uriToLegacySigel(library);
                if (profileString.contains("extrafields=")) {
                    String[] lines = profileString.split("\n");
                    StringBuilder sb = new StringBuilder();
                    for (String line : lines) {
                        sb.append(line);
                        if (line.startsWith("extrafields=")) {
                            if (!line.endsWith(";")) {
                                sb.append(";");
                            }
                            sb.append(sigel + ":852,856;");
                        }
                        sb.append("\n");
                    }
                    profileString = sb.toString();
                }
                else {
                    profileString = "extrafields=" + sigel + ":852,856;" + "\n" + profileString;
                }
            }

            File tempFile = File.createTempFile("profile", ".tmp");
            try (FileWriter fw = new FileWriter(tempFile)) {
                fw.write(profileString);
            }
            ExportProfile profile = new ExportProfile(tempFile);
            tempFile.delete();

            Vector<MarcRecord> result = MarcExport.compileVirtualMarcRecord(profile, rootDocument, whelk, toMarcXmlConverter);

            response.setContentType("application/octet-stream");
            response.setHeader("Cache-Control", "no-cache");
            response.setHeader("Content-Disposition", "attachment; filename=\"libris_marc_" + rootDocument.getShortId() + "\"");
            MarcRecordWriter writer = profile.getProperty("format", "ISO2709").equalsIgnoreCase("MARCXML") ?
                    new MarcXmlRecordWriter(response.getOutputStream(), "UTF-8") :
                    new Iso2709MarcRecordWriter(response.getOutputStream(), "UTF-8");
            for (MarcRecord record : result) {
                writer.writeRecord(record);
            }
            writer.close();
            response.getOutputStream().close();
        } catch (Throwable e) {
            log.error("Failed handling _compilemarc request: ", e);
            throw e;
        }
    }
}

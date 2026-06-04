package whelk.sru.servlet;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.io.IOUtils;
import org.apache.cxf.staxutils.StaxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.export.ExportProfile;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcXmlRecordWriter;
import whelk.Document;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.ESSettings;
import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.ResourceLookup;
import whelk.sru.cql.Translation;
import whelk.util.MarcExport;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.*;

// Test locally like so:
// curl "http://localhost:8187/?operation=searchRetrieve&query=isbn=9789130008650"
// (Elastic must be running)
public class SruServlet extends WhelkHttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());

    private static final List<String> SUPPORTED_VERSIONS = List.of("1.0", "1.1", "1.2");

    JsonLD2MarcXMLConverter converter;
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    ResourceLookup resourceLookup;
    ESSettings esSettings;

    String explain = loadResource("explain.xml");
    ExportProfile marcExportProfile;

    @Override
    protected void init(Whelk whelk) {
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        resourceLookup = ResourceLookup.load(whelk);
        esSettings = new ESSettings(whelk);

        Properties marcProperties = new Properties();
        marcExportProfile = new ExportProfile(marcProperties);

        try {
            marcProperties.load(new StringReader(loadResource("websok.properties")));
        } catch (IOException e) {
            logger.error("Could not read MARC profile from jar resources.", e);
        }
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        // Same as the old version
        res.setCharacterEncoding("utf-8");
        res.setContentType("text/xml");

        Map<String, String[]> parameters = req.getParameterMap();

        var version = getParameter(parameters, "version");
        if (version == null) {
            version = "1.2";
        }

        if (!SUPPORTED_VERSIONS.contains(version)) {
            String unsupported = loadResource("unsupported-version.xml");
            res.setStatus(200);
            var writer = new PrintWriter(new BufferedOutputStream(res.getOutputStream()));
            writer.print(unsupported);
            writer.flush();
            writer.close();
            return;
        }

        var operation = getParameter(parameters, "operation");

        if (operation == null || "explain".equals(operation)) {
            sendXml(res, 200, explain, version);
            return;
        }

        if ( !"searchRetrieve".equals(operation)) {
            logger.debug("Bad SRU query (operation/searchRetrieve expected): {}", parameters);
            sendXml(res, 400, loadResource("unsupported-operation.xml"), version);
            return;
        }

        if ( !parameters.containsKey("query") ) {
            logger.debug("Bad SRU query: {}", parameters);
            sendXml(res, 400, loadResource("missing-query.xml"), version);
            return;
        }

        int startRecord = 1;
        if ( parameters.containsKey("startRecord") ) {

            startRecord = 1 + Integer.parseInt(getParameter(parameters, "startRecord"));
        }

        int maximumRecords = 10;
        if ( parameters.containsKey("maximumRecords") ) {

            maximumRecords = Integer.parseInt(getParameter(parameters, "maximumRecords"));
        }

        Map<String, Object> results;
        try {
            String CqlQueryString = getParameter(parameters, "query");
            String XlQueryString = Translation.translateCqlToXlQuery(CqlQueryString);

            // This part is a little weird
            HashMap<String, String[]> paramsAsIfSearch = new HashMap<>();
            String[] q = new String[]{XlQueryString};
            paramsAsIfSearch.put("_q", q);
            paramsAsIfSearch.put("_stats", new String[] { "false" }); // don't need facets
            paramsAsIfSearch.put("_offset", new String[] {"" + (startRecord-1)});
            paramsAsIfSearch.put("_limit", new String[] {"" + maximumRecords});

            QueryParams qp = new QueryParams(paramsAsIfSearch);
            AppParams ap = new AppParams(new HashMap<>(), whelk.getJsonld());
            Query query = new Query(qp, ap, resourceLookup, esSettings, whelk);
            results = query.collectResults();
        } catch (InvalidQueryException | ParseCancellationException e) {
            logger.info("Bad query: \"" + parameters.get("query")[0] + "\" -> " + e.getMessage());
            sendXml(res, 400, loadResource("syntax-error.xml"), version);
            return;
        }

        // Like the pre-existing implementation, supply only up to 10 hits per query.
        List items = (List) results.get("items");
        if (items.size() > 10)
            items = items.subList(0, 10);

        // Build the xml response feed
        try {
            OutputStream out = res.getOutputStream();
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(out);

            writer.writeStartDocument("UTF-8", "1.0");
            writer.writeStartElement("searchRetrieveResponse");
            writer.writeAttribute("xmlns", "http://www.loc.gov/zing/srw/");

            writer.writeStartElement("version");
            writer.writeCharacters(version);
            writer.writeEndElement(); // version

            writer.writeStartElement("numberOfRecords");
            writer.writeCharacters("" + results.get("totalItems"));
            writer.writeEndElement(); // numberOfRecords

            writer.writeStartElement("records");

            for (Object o : items) {
                Map m = (Map) o;

                String systemID = whelk.getStorage().getSystemIdByIri( (String) m.get("@id"));
                Document embellished = whelk.loadEmbellished(systemID);

                writer.writeStartElement("record");

                writer.writeStartElement("recordIdentifier");
                writer.writeCharacters(embellished.getControlNumber()); // This is a MARC-protocol
                writer.writeEndElement(); // recordIdentifier

                writer.writeStartElement("recordPacking");
                writer.writeCharacters("xml");
                writer.writeEndElement(); // recordPacking

                writer.writeStartElement("recordSchema");
                writer.writeCharacters("info:srw/schema/1/marcxml-v1.1");
                writer.writeEndElement(); // recordSchema

                writer.writeStartElement("recordData");
                Vector<MarcRecord> marcRecords = MarcExport.compileVirtualMarcRecord(marcExportProfile, embellished, whelk, converter);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                MarcXmlRecordWriter stringOutput = new MarcXmlRecordWriter(baos, "UTF-8", false);
                for (MarcRecord mr : marcRecords) {
                    stringOutput.writeRecord(mr);
                }
                stringOutput.close();
                StaxUtils.copy(xmlInputFactory.createXMLStreamReader(new StringReader(baos.toString())), writer);
                writer.writeEndElement(); // recordData

                writer.writeEndElement(); // record
            }

            writer.writeEndElement(); // records
            writer.writeEndElement(); // searchRetrieveResponse
            writer.writeEndDocument();

            writer.close();
            out.flush();
            out.close();
        } catch (XMLStreamException e) {
            logger.error("Couldn't build SRU response.", e);
        }
    }

    private static void sendXml(HttpServletResponse res, int status, String xml, String version) throws IOException {
        if (!"1.2".equals(version)) {
            xml = xml.replace("<version>1.2</version>", "<version>" + version + "</version>");
            xml = xml.replace("<zs:version>1.2</zs:version>", "<zs:version>" + version + "</zs:version>");
        }

        res.setStatus(status);
        var writer = new PrintWriter(new BufferedOutputStream(res.getOutputStream()));
        writer.print(xml);
        writer.flush();
        writer.close();
    }

    private static String getParameter(Map<String, String[]> parameters, String name) {
        if (!parameters.containsKey(name)) {
            return null;
        }
        var parameter = parameters.get(name);
        if (parameter.length != 1) {
            return null;
        }
        return parameter[0];
    }

    private static String loadResource(String name) {
        var path = "sru/" + name;
        try (InputStream scriptStream = SruServlet.class.getClassLoader().getResourceAsStream(path)) {
            assert scriptStream != null;
            return IOUtils.toString(new InputStreamReader(scriptStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
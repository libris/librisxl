package whelk.sru.servlet;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.io.IOUtils;
import org.apache.cxf.staxutils.StaxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.ESSettings;
import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.ResourceLookup;
import whelk.sru.cql.Translation;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Test locally like so:
// curl "http://localhost:8187/?operation=searchRetrieve&query=isbn=9789130008650"
// (Elastic must be running)
public class SruServlet extends WhelkHttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());

    JsonLD2MarcXMLConverter converter;
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    ResourceLookup resourceLookup;
    ESSettings esSettings;

    String explain = loadClassPathResource("static-sru-explain.xml");

    @Override
    protected void init(Whelk whelk) {
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        resourceLookup = ResourceLookup.load(whelk);
        esSettings = new ESSettings(whelk);
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        // Same as the old version
        res.setCharacterEncoding("utf-8");
        res.setContentType("text/xml");

        Map<String, String[]> parameters = req.getParameterMap();
        var operation = getParameter(parameters, "operation");

        if (parameters.isEmpty() || "explain".equals(operation)) {
            res.setStatus(200);
            var writer = new PrintWriter(new BufferedOutputStream(res.getOutputStream()));
            writer.print(explain);
            writer.flush();
            writer.close();
            return;
        }

        if ( operation == null || !parameters.containsKey("query") ) {
            logger.debug("Bad SRU query: {}", parameters);
            res.sendError(400);
            return;
        }

        if ( !"searchRetrieve".equals(operation)) {
            logger.debug("Bad SRU query (operation/searchRetrieve expected): {}", parameters);
            res.sendError(400);
            return;
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
            QueryParams qp = new QueryParams(paramsAsIfSearch);
            AppParams ap = new AppParams(new HashMap<>(), whelk.getJsonld());
            Query query = new Query(qp, ap, resourceLookup, esSettings, whelk);
            results = query.collectResults();
        } catch (InvalidQueryException | ParseCancellationException e) {
            logger.info("Bad query: \"" + parameters.get("query")[0] + "\" -> " + e.getMessage());
            res.sendError(400);
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
            writer.writeCharacters("1.2");
            writer.writeEndElement(); // version

            writer.writeStartElement("numberOfRecords");
            writer.writeCharacters("" + results.get("totalItems"));
            writer.writeEndElement(); // numberOfRecords

            writer.writeStartElement("records");

            for (Object o : items) {
                Map m = (Map) o;

                String systemID = whelk.getStorage().getSystemIdByIri( (String) m.get("@id"));
                Document embellished = whelk.loadEmbellished(systemID);
                String convertedText = (String) converter.convert(embellished.data, embellished.getShortId()).get(JsonLd.NON_JSON_CONTENT_KEY);

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
                StaxUtils.copy(xmlInputFactory.createXMLStreamReader(new StringReader(convertedText)), writer);
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

    private static String loadClassPathResource(String name) {
        try (InputStream scriptStream = SruServlet.class.getClassLoader().getResourceAsStream(name)) {
            assert scriptStream != null;
            return IOUtils.toString(new InputStreamReader(scriptStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
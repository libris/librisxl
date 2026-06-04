package whelk.sru.servlet;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.cxf.staxutils.StaxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;
import whelk.sru.cql.Translation;
import whelk.util.http.WhelkHttpServlet;
//import javax.xml.transform.stax.StAXResult;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.Transformer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.InputStreamReader;
import java.util.*;

// Test locally like so:
// curl "http://localhost:8187/?operation=searchRetrieve&query=isbn=9789130008650"
// (Elastic must be running)
public class SruServlet extends WhelkHttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());

    JsonLD2MarcXMLConverter converter;
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    ResourceLookup resourceLookup;
    ESSettings esSettings;
    private Formats formats = null;

    @Override
    protected void init(Whelk whelk) {
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        resourceLookup = ResourceLookup.load(whelk);
        esSettings = new ESSettings(whelk);
	formats = new Formats();
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {

        // Same as the old version
        res.setCharacterEncoding("utf-8");
        res.setContentType("text/xml");

        Map<String, String[]> parameters = req.getParameterMap();

        if ( !parameters.containsKey("operation") || !parameters.containsKey("query") ) {
            logger.debug("Bad SRU query: " + parameters);
            res.sendError(400);
            return;
        }

        if ( parameters.get("operation").length != 1 || !parameters.get("operation")[0].equals("searchRetrieve") ) {
            logger.debug("Bad SRU query (operation/searchRetrieve expected): " + parameters);
            res.sendError(400);
            return;
        }

        Map<String, Object> results;
        String format;  

        try {
            String CqlQueryString = parameters.get("query")[0];
            format = parameters.get("recordSchema")[0];
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

	Transformer transformer = null;
	String recordsschema = "";

	try {
	switch (Formats.FORMATS.getOrDefault(format, Formats.Format.MARC_XML)) {
		case MARC_XML -> { transformer = null; recordsschema = "marcxml-v1.1"; }
                case MODS -> { transformer = formats.transformers.get(Formats.Format.MODS).newTransformer(); recordsschema = "mods-v3.0"; }
                case DC -> { transformer = formats.transformers.get(Formats.Format.DC).newTransformer(); recordsschema = "dc-v1.1"; }
		case UNSUPPORTED -> { transformer = null; recordsschema = "marcxml-v1.1"; }
        }
	}
	catch (TransformerException e){
            logger.info(e.getMessage());
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

            writer.writeStartElement("version");
            writer.writeCharacters("1.2");
            writer.writeEndElement(); // version

            writer.writeStartElement("numberOfRecords");
            writer.writeCharacters("" + results.get("totalItems"));
            writer.writeEndElement(); // numberOfRecords

            writer.writeStartElement("records");
	    //writer.flush();

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
                writer.writeCharacters("info:srw/schema/1/"+recordsschema);
                writer.writeEndElement(); // recordSchema

                //writer.writeStartElement("recordData");
	    	writer.flush();

		out.write("<recordData>".getBytes("UTF-8"));

		if ( transformer == null ) {
                	StaxUtils.copy(xmlInputFactory.createXMLStreamReader(new StringReader(convertedText)), writer);
		} else {
			try {
        			transformer.transform(new StreamSource(new StringReader(convertedText)), new StreamResult(out));
			}
			catch (TransformerException e) {
            			logger.info(e.getMessage());
            			res.sendError(400);
            			return;
			}
		}
		out.write("</recordData>".getBytes("UTF-8"));
                out.flush();

                //writer.writeEndElement(); // recordData

                writer.writeEndElement(); // record
	    	writer.flush();
            }

            writer.writeEndElement(); // records
            writer.writeEndElement(); // searchRetrieveResponse
            writer.writeEndDocument();

	    writer.flush();
            writer.close();
            out.flush();
            out.close();
        } catch (XMLStreamException e) {
            logger.error("Couldn't build SRU response.", e);
        }
    }
}

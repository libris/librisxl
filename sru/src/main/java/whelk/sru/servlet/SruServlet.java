package whelk.sru.servlet;

import org.apache.cxf.staxutils.StaxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;
import whelk.util.WhelkFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class SruServlet extends HttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());

    Whelk whelk;
    JsonLD2MarcXMLConverter converter;
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    VocabMappings vocabMappings;
    ESSettings esSettings;

    public void init() {
        whelk = WhelkFactory.getSingletonWhelk();
        converter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter());
        vocabMappings = new VocabMappings(whelk);
        esSettings = new ESSettings(whelk);
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {

        // Same as the old version
        res.setCharacterEncoding("utf-8");
        res.setContentType("text/xml");

        Map<String, String[]> parameters = req.getParameterMap();

        if ( !parameters.containsKey("operation") || !parameters.containsKey("query") ) {
            logger.debug("Bad SRU query: " + parameters);
            res.sendError(400);
        }

        if ( parameters.get("operation").length != 1 || !parameters.get("operation")[0].equals("searchRetrieve") ) {
            logger.debug("Bad SRU query (operation/searchRetrieve expected): " + parameters);
            res.sendError(400);
        }

        String queryString = parameters.get("query")[0];
        String instanceOnlyQueryString = "(" + queryString + ") AND type=Instance";

        Map<String, Object> results;
        try {
            // This part is a little weird
            HashMap<String, String[]> paramsAsIfSearch = new HashMap<>();
            String[] q = new String[]{instanceOnlyQueryString};
            paramsAsIfSearch.put("_q", q);
            QueryParams qp = new QueryParams(paramsAsIfSearch);
            AppParams ap = new AppParams(new HashMap<>(), qp);
            Query query = new Query(qp, ap, vocabMappings, esSettings, whelk);
            results = query.collectResults();
        } catch (InvalidQueryException e) {
            logger.error("Bad query.", e);
            res.sendError(400);
            return;
        }

        // Like the pre-existing implementation, supply only up to 10 hits per query.
        List items = (List) results.get("items");
        if (items.size() > 10)
            items = items.subList(0, 10);

        // Build the xml response feed
        try {
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

            writer.writeStartDocument("UTF-8", "1.0");
            writer.writeStartElement("searchRetrieveResponse");

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
        } catch (XMLStreamException e) {
            logger.error("Couldn't build SRU response.", e);
            return;
        }
    }

}
package whelk.sru.servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;
import whelk.search2.querytree.QueryTree;
import whelk.util.WhelkFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SruServlet extends HttpServlet {
    private final Logger logger = LogManager.getLogger(this.getClass());

    Whelk whelk;

    public void init() {
        whelk = WhelkFactory.getSingletonWhelk();
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        Map<String, String[]> parameters = req.getParameterMap();

        if ( !parameters.containsKey("operation") || !parameters.containsKey("query") ) {
            res.sendError(400); // DEAL WITH IN A BETTER WAY
        }

        if ( parameters.get("operation").length != 1 || !parameters.get("operation")[0].equals("searchRetrieve") ) {
            res.sendError(400); // DEAL WITH IN A BETTER WAY
        }

        String queryString = parameters.get("query")[0];

        System.err.println("Full/correct query? " + parameters.get("query")[0]);

        Map<String, Object> results;
        try {
            // Just.. WOW.
            HashMap<String, String[]> paramsAsIfSearch = new HashMap<>();
            String[] q = new String[]{queryString};
            paramsAsIfSearch.put("_q", q);
            QueryParams qp = new QueryParams(paramsAsIfSearch);
            AppParams ap = new AppParams(new HashMap<>(), qp);
            Query query = new Query(qp, ap, new VocabMappings(whelk), new ESSettings(whelk), whelk);
            results = query.collectResults();
        } catch (InvalidQueryException e) {
            logger.error("Bad query.", e);
            res.sendError(400);
            return;
        }

        //System.err.println(results);
        List items = (List) results.get("items");
        for (Object o : items) {
            Map m = (Map) o;
            System.err.println(m.get("@id"));
        }

        // Build the xml response feed
        try {
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

            writer.writeStartDocument("UTF-8", "1.0");
            writer.writeStartElement("WHATEVER");

            writer.writeEndElement();
            writer.writeEndDocument();

            writer.close();
        } catch (XMLStreamException e) {
            logger.error("Couldn't build SRU response.", e);
            return;
        }
    }

}
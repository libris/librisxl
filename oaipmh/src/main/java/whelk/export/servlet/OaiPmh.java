package whelk.export.servlet;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.converter.FormatConverter;
import whelk.converter.JsonLD2DublinCoreConverter;
import whelk.converter.JsonLD2RdfXml;
import whelk.converter.marc.JsonLD2MarcXMLConverter;
import whelk.util.PropertyLoader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;


public class OaiPmh extends HttpServlet
{
    // OAI-PMH Error/Condition codes
    public final static String OAIPMH_ERROR_BAD_VERB = "badVerb";
    public final static String OAIPMH_ERROR_BAD_RESUMPTION_TOKEN = "badResumptionToken";
    public final static String OAIPMH_ERROR_BAD_ARGUMENT = "badArgument";
    public final static String OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT = "cannotDisseminateFormat";
    public final static String OAIPMH_ERROR_ID_DOES_NOT_EXIST = "idDoesNotExist";
    public final static String OAIPMH_ERROR_NO_RECORDS_MATCH = "noRecordsMatch";
    public final static String OAIPMH_ERROR_NO_METADATA_FORMATS = "noMetadataFormats";
    public final static String OAIPMH_ERROR_NO_SET_HIERARCHY = "noSetHierarchy";

    static final Counter requests = Counter.build()
            .name("oaipmh_requests_total").help("Total requests to OAIPMH.")
            .labelNames("verb").register();

    static final Gauge ongoingRequests = Gauge.build()
            .name("oaipmh_ongoing_requests_total").help("Total ongoing OAIPMH requests.")
            .labelNames("verb").register();

    static final Summary requestsLatency = Summary.build()
            .name("oaipmh_requests_latency_seconds")
            .help("OAIPMH request latency in seconds.")
            .labelNames("verb").register();

    static final Counter badRequests = Counter.build()
            .name("oaipmh_bad_request_total").help("Total bad requests.")
            .labelNames("error").register();

    // Supported OAI-PMH metadata formats
    public static class FormatDescription
    {
        public FormatDescription(FormatConverter converter, boolean isXmlFormat, String xmlSchema, String xmlNamespace) {
            this.converter = converter;
            this.isXmlFormat = isXmlFormat;
            this.xmlSchema = xmlSchema;
            this.xmlNamespace = xmlNamespace;
        }
        public final FormatConverter converter;
        public final boolean isXmlFormat;
        public final String xmlSchema;
        public final String xmlNamespace;
    }
    public final static HashMap<String, FormatDescription> supportedFormats;
    public final static String FORMAT_EXPANDED_POSTFIX = "_expanded";
    public final static String FORMAT_INCLUDE_HOLD_POSTFIX = "_includehold";
    static
    {
        supportedFormats = new HashMap<String, FormatDescription>();
        supportedFormats.put("oai_dc", new FormatDescription(new JsonLD2DublinCoreConverter(), true, "http://www.openarchives.org/OAI/2.0/oai_dc.xsd", "http://www.openarchives.org/OAI/2.0"));
        supportedFormats.put("marcxml", new FormatDescription(new JsonLD2MarcXMLConverter(), true, "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd", "http://www.loc.gov/MARC21/slim"));
        supportedFormats.put("rdfxml", new FormatDescription(new JsonLD2RdfXml(), true, null, null));
        supportedFormats.put("jsonld", new FormatDescription(null, false, null, null));

        // Add all formats with the "_includehold" and "_expanded" postfixes
        for (String format : new String[] {"marcxml", "oai_dc", "rdfxml", "jsonld"})
        {
            supportedFormats.put(format+FORMAT_INCLUDE_HOLD_POSTFIX, supportedFormats.get(format));
            supportedFormats.put(format+FORMAT_EXPANDED_POSTFIX, supportedFormats.get(format));
            supportedFormats.put(format+FORMAT_INCLUDE_HOLD_POSTFIX+FORMAT_EXPANDED_POSTFIX, supportedFormats.get(format));
        }
    }

    public static Properties configuration;
    public static PostgreSQLComponent s_postgreSqlComponent;
    public static JsonLd s_jsonld; // For model driven behaviour
    public static Whelk s_whelk;
    private final Logger logger = LogManager.getLogger(this.getClass());

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        handleRequest(req, res);
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        handleRequest(req, res);
    }

    public void init()
    {
        configuration = PropertyLoader.loadProperties("secret");
        s_postgreSqlComponent = new PostgreSQLComponent(configuration.getProperty("sqlUrl"), configuration.getProperty("sqlMaintable"));

        s_whelk = new Whelk(s_postgreSqlComponent);
        s_whelk.loadCoreData();
        Map displayData = s_whelk.getDisplayData();
        Map vocabData = s_whelk.getVocabData();
        s_jsonld = new JsonLd(displayData, vocabData);
    }

    public void destroy()
    {
    }

    private void handleRequest(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        String verb = req.getParameter("verb");
        if (verb == null)
            verb = "";

        logger.info("Received request with verb: {} from {}:{}.", verb, req.getRemoteAddr(), req.getRemotePort());

        res.setContentType("text/xml");

        requests.labels(verb).inc();
        ongoingRequests.labels(verb).inc();
        Summary.Timer requestTimer = requestsLatency.labels(verb).startTimer();

        try
        {
            switch (verb) {
                case "GetRecord":
                    GetRecord.handleGetRecordRequest(req, res);
                    break;
                case "Identify":
                    Identify.handleIdentifyRequest(req, res);
                    break;
                case "ListIdentifiers":
                    // ListIdentifiers is (just about) identical to ListRecords, except that metadata bodies are omitted
                    ListRecords.handleListRecordsRequest(req, res, true);
                    break;
                case "ListMetadataFormats":
                    ListMetadataFormats.handleListMetadataFormatsRequest(req, res);
                    break;
                case "ListRecords":
                    ListRecords.handleListRecordsRequest(req, res, false);
                    break;
                case "ListSets":
                    ListSets.handleListSetsRequest(req, res);
                    break;
                default:
                    badRequests.labels(OAIPMH_ERROR_BAD_VERB).inc();
                    ResponseCommon.sendOaiPmhError(OAIPMH_ERROR_BAD_VERB, "OAI-PMH verb must be one of [GetRecord, Identify, " +
                            "ListIdentifiers, ListMetadataFormats, ListRecords, ListSets].", req, res);
            }
        }
        catch (IOException | XMLStreamException e)
        {
            // These exceptions are to be expected in every case where a client/harvester closes or loses connection
            // while a response is being sent.
            logger.warn("Broken client pipe {}:{}, response feed interrupted.", req.getRemoteAddr(), req.getRemotePort(), e);
        }
        catch (SQLException e)
        {
            logger.error("Database error.", e);
            res.sendError(500);
        }
        finally {
            ongoingRequests.labels(verb).dec();
            requestTimer.observeDuration();
        }
    }
}

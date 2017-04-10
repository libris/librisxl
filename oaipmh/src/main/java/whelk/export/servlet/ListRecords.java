package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;

import io.prometheus.client.Counter;

public class ListRecords
{
    private final static String FROM_PARAM = "from";
    private final static String UNTIL_PARAM = "until";
    private final static String SET_PARAM = "set";
    private final static String RESUMPTION_PARAM = "resumptionToken";
    private final static String FORMAT_PARAM = "metadataPrefix";

    private static final Counter failedRequests = Counter.build()
            .name("oaipmh_failed_listrecords_requests_total").help("Total failed ListRecords requests.")
            .labelNames("error").register();


    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'ListRecords', sends a proper response.
     * @param onlyIdentifiers When this is set to true, the response will be formatted as a ListIdentifiers response.
     *                        When it is false, the response will be formatted as a ListRecords response.
     */
    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response, boolean onlyIdentifiers)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String from = request.getParameter(FROM_PARAM); // optional
        String until = request.getParameter(UNTIL_PARAM); // optional
        String set = request.getParameter(SET_PARAM); // optional
        String resumptionToken = request.getParameter(RESUMPTION_PARAM); // exclusive, not supported/used
        String metadataPrefix = request.getParameter(FORMAT_PARAM); // required

        if (ResponseCommon.errorOnExtraParameters(request, response,
                FROM_PARAM, UNTIL_PARAM, SET_PARAM, RESUMPTION_PARAM, FORMAT_PARAM))
            return;

        // We do not use resumption tokens.
        if (resumptionToken != null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN,
                    "No such resumption token was issued", request, response);
            return;
        }

        if (metadataPrefix == null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        // Was the set selection valid?
        SetSpec setSpec = new SetSpec(set);
        if (!setSpec.isValid())
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Not a supported set spec: " + set, request, response);
            return;
        }

        // Was the data ordered in a format we know?
        if (!OaiPmh.supportedFormats.keySet().contains(metadataPrefix))
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT, "Unsupported format: " + metadataPrefix,
                    request, response);
            return;
        }

        ZonedDateTime fromDateTime = Helpers.parseISO8601(from);
        ZonedDateTime untilDateTime = Helpers.parseISO8601(until);

        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection())
        {
            dbconn.setAutoCommit(false);
            try (PreparedStatement preparedStatement = Helpers.getMatchingDocumentsStatement(dbconn, fromDateTime, untilDateTime, setSpec);
                 ResultSet resultSet = preparedStatement.executeQuery())
            {
                respond(request, response, metadataPrefix, onlyIdentifiers,
                        metadataPrefix.endsWith(OaiPmh.FORMAT_EXPANDED_POSTFIX), resultSet);
            } finally {
                dbconn.commit();
            }
        }
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                                String requestedFormat, boolean onlyIdentifiers, boolean embellish, ResultSet resultSet)
            throws IOException, XMLStreamException, SQLException
    {
        // Is the resultset empty?
        if (!resultSet.isBeforeFirst())
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
            return;
        }

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        ResponseCommon.writeOaiPmhHeader(writer, request, true);

        if (onlyIdentifiers)
            writer.writeStartElement("ListIdentifiers");
        else
            writer.writeStartElement("ListRecords");

        while (resultSet.next())
        {
            ResponseCommon.emitRecord(resultSet, writer, requestedFormat, onlyIdentifiers, embellish);
        }

        writer.writeEndElement(); // ListIdentifiers/ListRecords
        ResponseCommon.writeOaiPmhClose(writer, request);
    }
}

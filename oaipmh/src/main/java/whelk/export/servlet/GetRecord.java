package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;

import io.prometheus.client.Counter;

public class GetRecord
{
    private final static String IDENTIFIER_PARAM = "identifier";
    private final static String FORMAT_PARAM = "metadataPrefix";
    private final static String DELETED_DATA_PARAM = "x-withDeletedData";

    private static final Counter failedRequests = Counter.build()
            .name("oaipmh_failed_getrecord_requests_total").help("Total failed GetRecord requests.")
            .labelNames("error").register();

    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'GetRecord' and sends a proper response.
     */
    public static void handleGetRecordRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String identifierUri = request.getParameter(IDENTIFIER_PARAM); // required
        String metadataPrefix = request.getParameter(FORMAT_PARAM); // required

        // optional and not technically legal OAI-PMH
        boolean withDeletedData = Boolean.parseBoolean(request.getParameter(DELETED_DATA_PARAM));

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM, FORMAT_PARAM, DELETED_DATA_PARAM))
            return;

        if (metadataPrefix == null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        if (!OaiPmh.supportedFormats.keySet().contains(metadataPrefix))
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT, "Unsupported format: " + metadataPrefix,
                    request, response);
            return;
        }

        if (identifierUri == null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "identifier argument required.", request, response);
            return;
        }

        String id = null;
        try (Connection dbconn = OaiPmh.s_whelk.getStorage().getConnection();
             PreparedStatement preparedStatement = Helpers.prepareSameAsStatement(dbconn, identifierUri);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (resultSet.next())
                id = resultSet.getString("id");
        }
        if (id == null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
            return;
        }

        try (Connection dbconn = OaiPmh.s_whelk.getStorage().getConnection())
        {
            dbconn.setAutoCommit(false);
            try (Helpers.ResultIterator it = Helpers.getMatchingDocuments(dbconn, null, null, null, id, false))
            {
                if (!it.hasNext())
                {
                    failedRequests.labels(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH).inc();
                    ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
                    return;
                }

                // Build the xml response feed
                XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
                XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

                ResponseCommon.writeOaiPmhHeader(writer, request, true);
                writer.writeStartElement("GetRecord");

                ResponseCommon.emitRecord(it.next(), writer, metadataPrefix, false,
                        metadataPrefix.contains(OaiPmh.FORMAT_EXPANDED_POSTFIX), withDeletedData);

                writer.writeEndElement(); // GetRecord
                ResponseCommon.writeOaiPmhClose(writer, request);
            } finally {
                dbconn.commit();
            }
        }
    }
}
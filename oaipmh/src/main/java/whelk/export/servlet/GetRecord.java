package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import io.prometheus.client.Counter;

public class GetRecord
{
    private final static String IDENTIFIER_PARAM = "identifier";
    private final static String FORMAT_PARAM = "metadataPrefix";

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

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM, FORMAT_PARAM))
            return;

        if (metadataPrefix == null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
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
        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = Helpers.prepareSameAsStatement(dbconn, identifierUri);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (resultSet.next())
                id = resultSet.getString("id");
        }

        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = prepareMatchingDocumentStatement(dbconn, id);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (!resultSet.next())
            {
                failedRequests.labels(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST).inc();
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                return;
            }

            ObjectMapper mapper = new ObjectMapper();
            String data = resultSet.getString("data");
            ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
            HashMap datamap = mapper.readValue(data, HashMap.class);
            Document document = new Document(datamap);

            // Expanded format requested, we need to build trees.
            if (metadataPrefix.endsWith(OaiPmh.FORMAT_EXPANDED_POSTFIX))
            {
                List<String> nodeDatas = new LinkedList<String>();
                HashSet<String> visitedIDs = new HashSet<String>();
                ListRecordTrees.ModificationTimes modificationTimes = new ListRecordTrees.ModificationTimes();
                modificationTimes.earliestModification = modified;
                modificationTimes.latestModification = modified;

                ListRecordTrees.addNodeAndSubnodesToTree(id, visitedIDs, nodeDatas, modificationTimes);

                // Value of modificationTimes.latestModification will have changed during tree building.
                modified = modificationTimes.latestModification;

                document = ListRecordTrees.mergeDocument(id, nodeDatas);
            }

            // Build the xml response feed
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

            ResponseCommon.writeOaiPmhHeader(writer, request, true);
            writer.writeStartElement("GetRecord");

            ResponseCommon.emitRecord(resultSet, document, writer, metadataPrefix, false);

            writer.writeEndElement(); // GetRecord
            ResponseCommon.writeOaiPmhClose(writer, request);
        }
    }

    private static PreparedStatement prepareMatchingDocumentStatement(Connection dbconn, String id)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // Construct the query
        String selectSQL = "SELECT data, collection, modified, deleted, data#>>'{@graph,1,heldBy,@id}' AS sigel FROM " +
                tableName + " WHERE id = ? AND collection <> 'definitions' ";
        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }
}
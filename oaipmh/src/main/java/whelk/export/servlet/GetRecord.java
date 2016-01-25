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

public class GetRecord
{
    private final static String IDENTIFIER_PARAM = "identifier";
    private final static String FORMAT_PARAM = "metadataPrefix";

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
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        if (identifierUri == null)
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "identifier argument required.", request, response);
            return;
        }

        String id = Helpers.getShorthandDocumentId(identifierUri);

        try (Connection dbconn = DataBase.getConnection()) {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

            // Construct the query
            String selectSQL = "SELECT data, manifest, modified, deleted, data#>'{@graph,1,heldBy,notation}' AS sigel FROM " +
                    tableName + " WHERE id = ? ";
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
            preparedStatement.setString(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.next())
            {
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                return;
            }

            ObjectMapper mapper = new ObjectMapper();
            String data = resultSet.getString("data");
            String manifest = resultSet.getString("manifest");
            boolean deleted = resultSet.getBoolean("deleted");
            ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
            HashMap datamap = mapper.readValue(data, HashMap.class);
            HashMap manifestmap = mapper.readValue(manifest, HashMap.class);
            Document jsonLDdoc = new Document(datamap, manifestmap);

            // Expanded format requested, we need to build trees.
            if (metadataPrefix.endsWith(OaiPmh.FORMATEXPANDED_POSTFIX))
            {
                List<String> nodeDatas = new LinkedList<String>();
                HashSet<String> visitedIDs = new HashSet<String>();
                ListRecordTrees.ModificationTimes modificationTimes = new ListRecordTrees.ModificationTimes();
                modificationTimes.earliestModification = modified;
                modificationTimes.latestModification = modified;

                try (Connection secondConn = DataBase.getConnection()) {
                    ListRecordTrees.addNodeAndSubnodesToTree(id, visitedIDs, secondConn, nodeDatas, modificationTimes);
                }

                // Value of modificationTimes.latestModification will have changed during tree building.
                modified = modificationTimes.latestModification;

                jsonLDdoc = ListRecordTrees.mergeDocument(id, nodeDatas);
            }

            // Build the xml response feed
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

            ResponseCommon.writeOaiPmhHeader(writer, request, true);
            writer.writeStartElement("GetRecord");
            writer.writeStartElement("record");

            writer.writeStartElement("header");

            if (deleted)
                writer.writeAttribute("status", "deleted");

            writer.writeStartElement("identifier");
            writer.writeCharacters(identifierUri);
            writer.writeEndElement(); // identifier

            writer.writeStartElement("datestamp");
            writer.writeCharacters(modified.toString());
            writer.writeEndElement(); // datestamp

            writer.writeStartElement("setSpec");
            String dataset = (String) manifestmap.get("collection");
            writer.writeCharacters( dataset );
            writer.writeEndElement(); // setSpec

            writer.writeEndElement(); // header

            if (!deleted)
            {
                writer.writeStartElement("metadata");
                ResponseCommon.writeConvertedDocument(writer, metadataPrefix, jsonLDdoc);
                writer.writeEndElement(); // metadata
            }

            writer.writeEndElement(); // record
            writer.writeEndElement(); // GetRecord
            ResponseCommon.writeOaiPmhClose(writer, request);
        }
    }
}
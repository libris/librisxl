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
        String identifier = request.getParameter(IDENTIFIER_PARAM); // required
        String metadataPrefix = request.getParameter(FORMAT_PARAM); // required

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM, FORMAT_PARAM))
            return;

        if (metadataPrefix == null)
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        if (identifier == null)
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "identifier argument required.", request, response);
            return;
        }

        try (Connection dbconn = DataBase.getConnection()) {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

            // Construct the query
            String selectSQL = "SELECT data, manifest, created, deleted FROM " + tableName +
                    " WHERE id > ? ";
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
            preparedStatement.setString(1, identifier);
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
            HashMap datamap = mapper.readValue(data, HashMap.class);
            HashMap manifestmap = mapper.readValue(manifest, HashMap.class);
            Document jsonLDdoc = new Document(datamap, manifestmap);

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
            writer.writeCharacters(jsonLDdoc.getURI().toString());
            writer.writeEndElement(); // identifier

            writer.writeStartElement("datestamp");
            ZonedDateTime created = ZonedDateTime.ofInstant(resultSet.getTimestamp("created").toInstant(), ZoneOffset.UTC);
            writer.writeCharacters(created.toString());
            writer.writeEndElement(); // datestamp

            writer.writeStartElement("setSpec");
            String dataset = (String) manifestmap.get("dataset");
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
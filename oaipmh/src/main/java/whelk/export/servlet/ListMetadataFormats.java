package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;

public class ListMetadataFormats
{
    private final static String IDENTIFIER_PARAM = "identifier";

    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'ListMetadataFormats' and sends a proper response.
     */
    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String identifier = request.getParameter(IDENTIFIER_PARAM); // optional

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM))
            return;

        // Looking for the requested document is essentially redundant, since we offer all supported metadata formats
        // for all documents, but we will check for it anyway, since the OAI-PMH specification requires an error if the
        // document does not exist.
        if (identifier != null) {
            try (Connection dbconn = DataBase.getConnection()) {
                String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
                String selectSQL = "SELECT deleted FROM " + tableName + " WHERE id = ? ";
                PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
                String id = Helpers.getShorthandDocumentId(identifier);
                if (id == null) {
                    ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                    return;
                }
                preparedStatement.setString(1, id);
                ResultSet resultSet = preparedStatement.executeQuery();

                // If there was no such document
                if (!resultSet.isBeforeFirst()) {
                    ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
                    return;
                }
            }
        }

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        ResponseCommon.writeOaiPmhHeader(writer, request, true);
        writer.writeStartElement("ListMetadataFormats");

        for ( String metadataPrefix : OaiPmh.supportedFormats.keySet() )
        {
            writer.writeStartElement("metadataFormat");
            writer.writeStartElement("metadataPrefix");
            writer.writeCharacters(metadataPrefix);
            writer.writeEndElement(); // metadataPrefix

            OaiPmh.FormatDescription formatDescription = OaiPmh.supportedFormats.get(metadataPrefix);
            if (formatDescription.xmlSchema != null)
            {
                writer.writeStartElement("schema");
                writer.writeCharacters(formatDescription.xmlSchema);
                writer.writeEndElement(); // schema
            }

            if (formatDescription.xmlNamespace != null)
            {
                writer.writeStartElement("metadataNamespace");
                writer.writeCharacters(formatDescription.xmlNamespace);
                writer.writeEndElement(); // metadataNamespace
            }

            writer.writeEndElement(); // metadataFormat
        }
        writer.writeEndElement(); // ListMetadataFormats
        ResponseCommon.writeOaiPmhClose(writer);
    }
}

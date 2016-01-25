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
    public static void handleListMetadataFormatsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String identifier = request.getParameter(IDENTIFIER_PARAM); // optional

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM))
            return;

        if (identifier != null) {

            String id = Helpers.getShorthandDocumentId(identifier);
            if (id == null) {
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                return;
            }

            try (Connection dbconn = DataBase.getConnection()) {
                String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
                String selectSQL = "SELECT deleted FROM " + tableName + " WHERE id = ? ";
                PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
                preparedStatement.setString(1, id);
                ResultSet resultSet = preparedStatement.executeQuery();

                // If there was no such document
                if (resultSet.next())
                {
                    boolean recordDeleted = resultSet.getBoolean("deleted");
                    if (recordDeleted)
                    {
                        ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                        return;
                    }
                }
                else {
                    ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
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
            emitMetadataFormat(metadataPrefix, writer);
            emitMetadataFormat(metadataPrefix + OaiPmh.FORMATEXPANDED_POSTFIX, writer);
        }
        writer.writeEndElement(); // ListMetadataFormats
        ResponseCommon.writeOaiPmhClose(writer, request);
    }

    private static void emitMetadataFormat(String metadataPrefix, XMLStreamWriter writer)
            throws XMLStreamException
    {
        writer.writeStartElement("metadataFormat");
        writer.writeStartElement("metadataPrefix");
        writer.writeCharacters(metadataPrefix);
        writer.writeEndElement(); // metadataPrefix

        OaiPmh.FormatDescription formatDescription = OaiPmh.supportedFormats.get(
                metadataPrefix.replace(OaiPmh.FORMATEXPANDED_POSTFIX, ""));
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
}

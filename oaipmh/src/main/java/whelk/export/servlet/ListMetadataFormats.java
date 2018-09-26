package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;

import io.prometheus.client.Counter;

public class ListMetadataFormats
{
    private final static String IDENTIFIER_PARAM = "identifier";

    private static final Counter failedRequests = Counter.build()
            .name("oaipmh_failed_listmetadataformats_requests_total").help("Total failed ListMetadataFormats requests.")
            .labelNames("error").register();

    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'ListMetadataFormats' and sends a proper response.
     */
    public static void handleListMetadataFormatsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String identifierUri = request.getParameter(IDENTIFIER_PARAM); // optional

        if (ResponseCommon.errorOnExtraParameters(request, response, IDENTIFIER_PARAM))
            return;

        if (identifierUri != null)
        {
            String id = null;
            try (Connection dbconn = OaiPmh.s_whelk.getStorage().getConnection();
                 PreparedStatement preparedStatement = Helpers.prepareSameAsStatement(dbconn, identifierUri);
                 ResultSet resultSet = preparedStatement.executeQuery())
            {
                if (resultSet.next())
                    id = resultSet.getString("id");
            }

            try (Connection dbconn = OaiPmh.s_whelk.getStorage().getConnection();
                 PreparedStatement preparedStatement = prepareMatchingDocumentStatement(dbconn, id);
                 ResultSet resultSet = preparedStatement.executeQuery())
            {
                // If there was no such document
                if (resultSet.next())
                {
                    boolean recordDeleted = resultSet.getBoolean("deleted");
                    if (recordDeleted)
                    {
                        failedRequests.labels(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST).inc();
                        ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                        return;
                    }
                }
                else {
                    failedRequests.labels(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST).inc();
                    ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_ID_DOES_NOT_EXIST, "", request, response);
                    return;
                }
            }
        }

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        ResponseCommon.writeOaiPmhHeader(writer, request, true);
        writer.writeStartElement("ListMetadataFormats");

        for ( String metadataPrefix : OaiPmh.supportedFormats.keySet() )
        {
            emitMetadataFormat(metadataPrefix, writer);
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

    private static PreparedStatement prepareMatchingDocumentStatement(Connection dbconn, String id)
            throws SQLException
    {
        String selectSQL = "SELECT deleted FROM lddb WHERE id = ? AND manifest->>'collection' <> 'definitions'";
        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }
}

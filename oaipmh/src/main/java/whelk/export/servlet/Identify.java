package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import io.prometheus.client.Counter;

public class Identify
{
    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'Identify' and sends a proper response.
     */
    public static void handleIdentifyRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        if (ResponseCommon.errorOnExtraParameters(request, response))
            return;

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

        ResponseCommon.writeOaiPmhHeader(writer, request, true);
        writer.writeStartElement("Identify");

        writer.writeStartElement("repositoryName");
        writer.writeCharacters("Libris XL");
        writer.writeEndElement(); // repositoryName

        writer.writeStartElement("baseURL");
        writer.writeCharacters(request.getRequestURL().toString());
        writer.writeEndElement(); // baseURL

        writer.writeStartElement("protocolVersion");
        writer.writeCharacters("2.0");
        writer.writeEndElement(); // protocolVersion

        writer.writeStartElement("adminEmail");
        writer.writeCharacters("libris@kb.se");
        writer.writeEndElement(); // adminEmail

        writer.writeStartElement("earliestDatestamp");
        try (Connection dbconn = OaiPmh.s_whelk.getStorage().getConnection();
             PreparedStatement preparedStatement = prepareStatement(dbconn);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (resultSet.next()) {
                ZonedDateTime earliest = ZonedDateTime.ofInstant(resultSet.getTimestamp("earliest").toInstant(), ZoneOffset.UTC);
                writer.writeCharacters(earliest.toString());
            } else
                writer.writeCharacters(ZonedDateTime.now(ZoneOffset.UTC).toString());
        }
        writer.writeEndElement(); // earliestDatestamp

        writer.writeStartElement("deletedRecord");
        writer.writeCharacters("persistent");
        writer.writeEndElement(); // deletedRecord

        writer.writeStartElement("granularity");
        writer.writeCharacters("YYYY-MM-DDThh:mm:ssZ");
        writer.writeEndElement(); // granularity

        writer.writeEndElement(); // Identify
        ResponseCommon.writeOaiPmhClose(writer, request);
    }

    private static PreparedStatement prepareStatement(Connection dbconn)
            throws SQLException
    {
        // Construct the query
        String selectSQL = "SELECT MIN(modified) as earliest FROM lddb";
        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);

        return preparedStatement;
    }
}

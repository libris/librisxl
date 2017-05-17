package whelk.export.servlet;

import whelk.util.LegacyIntegrationTools;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;

import io.prometheus.client.Counter;

public class ListSets
{
    private final static String RESUMPTION_PARAM = "resumptionToken";

    private static final Counter failedRequests = Counter.build()
            .name("oaipmh_failed_listsets_requests_total").help("Total failed ListSets requests.")
            .labelNames("error").register();

    /**
     * Verifies the integrity of a OAI-PMH request with the verb 'ListSets' and sends a proper response.
     */
    public static void handleListSetsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String resumptionToken = request.getParameter(RESUMPTION_PARAM); // exclusive, not supported/used

        if (ResponseCommon.errorOnExtraParameters(request, response, RESUMPTION_PARAM))
            return;

        // We do not use resumption tokens.
        if (resumptionToken != null)
        {
            failedRequests.labels(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN).inc();
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN,
                    "No such resumption token was issued", request, response);
            return;
        }

        // Build the xml response feed
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
        XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream(), "UTF-8");

        ResponseCommon.writeOaiPmhHeader(writer, request, true);
        writer.writeStartElement("ListSets");

        // Static sets
        writer.writeStartElement("set");
        writer.writeStartElement("setSpec");
        writer.writeCharacters("auth");
        writer.writeEndElement(); // setSpec
        writer.writeStartElement("setName");
        writer.writeCharacters("Authority records");
        writer.writeEndElement(); // setName
        writer.writeEndElement(); // set

        writer.writeStartElement("set");
        writer.writeStartElement("setSpec");
        writer.writeCharacters("bib");
        writer.writeEndElement(); // setSpec
        writer.writeStartElement("setName");
        writer.writeCharacters("Bibliographic records");
        writer.writeEndElement(); // setName
        writer.writeEndElement(); // set

        writer.writeStartElement("set");
        writer.writeStartElement("setSpec");
        writer.writeCharacters("hold");
        writer.writeEndElement(); // setSpec
        writer.writeStartElement("setName");
        writer.writeCharacters("Holding records");
        writer.writeEndElement(); // setName
        writer.writeEndElement(); // set

        // Dynamic sigel-sets
        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = prepareStatement(dbconn);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            while (resultSet.next())
            {
                String sigel = resultSet.getString("sigel");
                if (sigel == null)
                    continue;
                sigel = LegacyIntegrationTools.uriToLegacySigel(sigel);
                if (sigel == null)
                    continue;

                writer.writeStartElement("set");
                writer.writeStartElement("setSpec");
                writer.writeCharacters("hold:"+sigel.replace("\"", ""));
                writer.writeEndElement(); // setSpec
                writer.writeStartElement("setName");
                writer.writeCharacters("Holding records for sigel: " + sigel);
                writer.writeEndElement(); // setName
                writer.writeEndElement(); // set

                writer.writeStartElement("set");
                writer.writeStartElement("setSpec");
                writer.writeCharacters("bib:"+sigel.replace("\"", ""));
                writer.writeEndElement(); // setSpec
                writer.writeStartElement("setName");
                writer.writeCharacters("Bibliographic records for sigel: " + sigel);
                writer.writeEndElement(); // setName
                writer.writeEndElement(); // set
            }
        }

        writer.writeEndElement(); // ListSets
        ResponseCommon.writeOaiPmhClose(writer, request);
    }

    private static PreparedStatement prepareStatement(Connection dbconn)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // Construct the query

        // The SELECT DISTINCT is semantically correct, but too slow because postgresql does not handle
        // loose indexscans, which can instead be simulated using the below query, courtesy of:
        // https://wiki.postgresql.org/wiki/Loose_indexscan

        /*String selectSQL = "SELECT DISTINCT data#>>'{@graph,1,heldBy,@id}' AS sigel FROM " + tableName +
                " WHERE collection = 'hold' ";*/

        String selectSQL = "WITH RECURSIVE t AS (\n" +
                "   SELECT MIN(data#>>'{@graph,1,heldBy,@id}') AS col FROM lddb\n" +
                "   UNION ALL\n" +
                "   SELECT (SELECT MIN(data#>>'{@graph,1,heldBy,@id}') FROM lddb WHERE data#>>'{@graph,1,heldBy,@id}' > t.col)\n" +
                "   FROM t WHERE t.col IS NOT NULL\n" +
                "   )\n" +
                "SELECT col as sigel FROM t WHERE col IS NOT NULL";

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setFetchSize(512);

        return preparedStatement;
    }
}

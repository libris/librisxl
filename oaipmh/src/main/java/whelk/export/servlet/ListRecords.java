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

public class ListRecords
{
    private final static String FROM_PARAM = "from";
    private final static String UNTIL_PARAM = "until";
    private final static String SET_PARAM = "set";
    private final static String RESUMPTION_PARAM = "resumptionToken";
    private final static String FORMAT_PARAM = "metadataPrefix";

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
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_RESUMPTION_TOKEN,
                    "No such resumption token was issued", request, response);
            return;
        }

        if (metadataPrefix == null)
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        // Was the set selection valid?
        SetSpec setSpec = new SetSpec(set);
        if (!setSpec.isValid())
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Not a supported set spec: " + set, request, response);
            return;
        }

        // Was the data ordered in a format we know?
        if (!OaiPmh.supportedFormats.keySet().contains(metadataPrefix.replace(OaiPmh.FORMATEXPANDED_POSTFIX, "")))
        {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT, "Unsupported format: " + metadataPrefix,
                    request, response);
            return;
        }

        // "No start date" is replaced with a _very_ early start date.
        if (from == null)
            from = "0000-01-01";

        ZonedDateTime fromDateTime = Helpers.parseISO8601(from);
        ZonedDateTime untilDateTime = Helpers.parseISO8601(until);

        if (metadataPrefix.endsWith(OaiPmh.FORMATEXPANDED_POSTFIX))
        {
            // Expand each record with its dependency tree
            ListRecordTrees.respond(request, response, fromDateTime, untilDateTime, setSpec, metadataPrefix);
        }
        else {
            // Normal record retrieval
            try (Connection dbconn = DataBase.getConnection()) {
                ResultSet resultSet = getMatchingDocuments(dbconn, fromDateTime, untilDateTime, setSpec);
                respond(request, response, metadataPrefix, onlyIdentifiers, resultSet);
            }
        }
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                                String requestedFormat, boolean onlyIdentifiers, ResultSet resultSet)
            throws IOException, XMLStreamException, SQLException
    {
        // An inelegant (but recommended) way of checking if the ResultSet is empty.
        // Avoids the need for "backing-up" which would prevent streaming of the ResultSet from the db.
        if (!resultSet.isBeforeFirst())
        {
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

        writer.writeStartElement("records");

        while (resultSet.next())
        {
            emitRecord(resultSet, writer, requestedFormat, onlyIdentifiers);
        }

        writer.writeEndElement(); // records
        writer.writeEndElement(); // ListIdentifiers/ListRecords
        ResponseCommon.writeOaiPmhClose(writer, request);
    }

    private static ResultSet getMatchingDocuments(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // Construct the query
        String selectSQL = "SELECT data, manifest, modified, deleted, " +
                " data#>'{@graph,1,heldBy,notation}' AS sigel FROM " +
                tableName +
                " WHERE modified > ? ";
        if (untilDateTime != null)
            selectSQL += " AND modified < ? ";
        if (setSpec.getRootSet() != null)
            selectSQL += " AND manifest->>'collection' = ? ";

        // Obviously query concatenation is dangerous business and should never be done, unfortunately JSONB fields
        // much like table names cannot be parameterized, and so there is little choice.
        if (setSpec.getSubset() != null)
            selectSQL += " AND data @> '{\"@graph\":[{\"heldBy\": {\"@type\": \"Organization\", \"notation\": \"" +
                    Helpers.scrubSQL(setSpec.getSubset()) + "\"}}]}' ";

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);

        // Assign parameters
        int parameterIndex = 1;
        preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
        if (untilDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
        if (setSpec.getRootSet() != null)
            preparedStatement.setString(parameterIndex++, setSpec.getRootSet());

        return preparedStatement.executeQuery();
    }

    private static void emitRecord(ResultSet resultSet, XMLStreamWriter writer, String requestedFormat, boolean onlyIdentifiers)
            throws SQLException, XMLStreamException, IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        String data = resultSet.getString("data");
        String manifest = resultSet.getString("manifest");
        boolean deleted = resultSet.getBoolean("deleted");
        String sigel = resultSet.getString("sigel");
        HashMap datamap = mapper.readValue(data, HashMap.class);
        HashMap manifestmap = mapper.readValue(manifest, HashMap.class);
        Document jsonLDdoc = new Document(datamap, manifestmap);

        writer.writeStartElement("record");

        writer.writeStartElement("header");

        if (deleted)
            writer.writeAttribute("status", "deleted");

        writer.writeStartElement("identifier");
        writer.writeCharacters(jsonLDdoc.getURI().toString());
        writer.writeEndElement(); // identifier

        writer.writeStartElement("datestamp");
        ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
        writer.writeCharacters(modified.toString());
        writer.writeEndElement(); // datestamp

        String dataset = (String) manifestmap.get("collection");
        if (dataset != null)
        {
            writer.writeStartElement("setSpec");
            writer.writeCharacters(dataset);
            writer.writeEndElement(); // setSpec
        }

        if (sigel != null)
        {
            writer.writeStartElement("setSpec");
            // Output sigel without quotation marks (").
            writer.writeCharacters(dataset + ":" + sigel.replace("\"", ""));
            writer.writeEndElement(); // setSpec
        }

        writer.writeEndElement(); // header

        if (!onlyIdentifiers && !deleted)
        {
            writer.writeStartElement("metadata");
            ResponseCommon.writeConvertedDocument(writer, requestedFormat, jsonLDdoc);
            writer.writeEndElement(); // metadata
        }

        writer.writeEndElement(); // record
    }
}

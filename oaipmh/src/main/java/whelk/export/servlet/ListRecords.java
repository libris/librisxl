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
import java.time.ZoneId;
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
     */
    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response, boolean onlyHeaders)
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
        if (!ResponseCommon.supportedFormats.contains(metadataPrefix))
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

        respond(request, response, fromDateTime, untilDateTime, setSpec, metadataPrefix, onlyHeaders);
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                         ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec,
                                String requestedFormat, boolean onlyHeaders)
            throws IOException, XMLStreamException, SQLException
    {
        try (Connection dbconn = DataBase.getConnection())
        {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

            // Construct the query
            String selectSQL = "SELECT data, manifest, created FROM " + tableName +
                    " WHERE created > ? ";
            if (untilDateTime != null)
                selectSQL += " AND created < ? ";
            if (setSpec.getRootSet() != null)
                selectSQL += " AND manifest->>'dataset' = ? ";

            selectSQL += " LIMIT 10 "; // TEMP
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);

            // Assign parameters
            int parameterIndex = 1;
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
            if (untilDateTime != null)
                preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
            if (setSpec.getRootSet() != null)
                preparedStatement.setString(parameterIndex++, setSpec.getRootSet());

            ResultSet resultSet = preparedStatement.executeQuery();

            // An inelegant (but recommended) way of checking if the ResultSet is empty.
            // Avoids the need for "backing-up" which would prevent streaming of the ResultSet from the db.
            if (!resultSet.isBeforeFirst())
            {
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
                return;
            }

            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());

            ResponseCommon.writeOaiPmhHeader(writer, request, true);
            writer.writeStartElement("records");

            ObjectMapper mapper = new ObjectMapper();
            while (resultSet.next())
            {
                String data = resultSet.getString("data");
                String manifest = resultSet.getString("manifest");
                HashMap datamap = mapper.readValue(data, HashMap.class);
                HashMap manifestmap = mapper.readValue(manifest, HashMap.class);
                Document jsonLDdoc = new Document(datamap, manifestmap);

                writer.writeStartElement("record");

                writer.writeStartElement("header");

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

                if (!onlyHeaders)
                {
                    writer.writeStartElement("metadata");
                    ResponseCommon.writeConvertedDocument(writer, requestedFormat, jsonLDdoc);
                    writer.writeEndElement(); // metadata
                }

                writer.writeEndElement(); // record
            }

            writer.writeEndElement(); // records
            ResponseCommon.writeOaiPmhClose(writer);
        }
    }
}

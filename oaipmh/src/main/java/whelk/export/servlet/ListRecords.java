package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;

public class ListRecords
{
    private final static String FROM_PARAM = "from";
    private final static String UNTIL_PARAM = "until";
    private final static String SET_PARAM = "set";
    private final static String RESUMPTION_PARAM = "resumptionToken";
    private final static String FORMAT_PARAM = "metadataPrefix";

    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        // Parse and verify the parameters allowed for this request
        String from = request.getParameter(FROM_PARAM); // optional
        String until = request.getParameter(UNTIL_PARAM); // optional
        String set = request.getParameter(SET_PARAM); // optional
        String resumptionToken = request.getParameter(RESUMPTION_PARAM); // exclusive, not supported/used
        String metadataPrefix = request.getParameter(FORMAT_PARAM); // required

        String unknownParameters = Helpers.getUnknownParameters(request,
                FROM_PARAM, UNTIL_PARAM, SET_PARAM, RESUMPTION_PARAM, FORMAT_PARAM);
        if (unknownParameters != null)
        {
            ResponseCommon.sendOaiPmhError("badArgument", "Request contained unknown parameter(s): " +
                    unknownParameters, request, response);
            return;
        }

        if (resumptionToken != null)
        {
            ResponseCommon.sendOaiPmhError("badResumptionToken", "No such resumption token was issued", request, response);
            return;
        }

        if (metadataPrefix == null)
        {
            ResponseCommon.sendOaiPmhError("badArgument", "metadataPrefix argument required.", request, response);
            return;
        }

        SetSpec setSpec = new SetSpec(set);
        if (!setSpec.isValid())
        {
            ResponseCommon.sendOaiPmhError("badArgument", "Not supported set spec: " + set, request, response);
            return;
        }

        // "No start date" is replaced with _very_ early start date.
        if (from == null)
            from = "0000-01-01";

        ZonedDateTime fromDateTime = Helpers.parseISO8601(from);
        ZonedDateTime untilDateTime = Helpers.parseISO8601(until);

        respond(request, response, fromDateTime, untilDateTime, setSpec);
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                         ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec)
            throws IOException, XMLStreamException, SQLException
    {
        try (Connection dbconn = DataBase.getConnection())
        {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

            // Construct the query
            String selectSQL = "SELECT data, manifest FROM " + tableName +
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

            ResponseCommon.streamResponse(resultSet, request, response);
        }
    }
}

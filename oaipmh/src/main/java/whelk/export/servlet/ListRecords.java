package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;

public class ListRecords {
    public static void handleListRecordsRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        String from = request.getParameter("from"); // optional
        String until = request.getParameter("until"); // optional
        String set = request.getParameter("set"); // optional
        String resumptionToken = request.getParameter("resumptionToken"); // exclusive, not supported/used
        String metadataPrefix = request.getParameter("metadataPrefix"); // required

        if (resumptionToken != null || metadataPrefix == null)
        {
            ResponseCommon.sendOaiPmhError("badArgument", "", request, response);
            return;
        }

        if (from == null)
            from = "0000-01-01";

        ZonedDateTime fromDateTime = ResponseCommon.parseISO8601(from);
        ZonedDateTime untilDateTime = ResponseCommon.parseISO8601(until);

        try (Connection dbconn = DataBase.getConnection())
        {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

            // Construct the query
            String selectSQL = "SELECT data, manifest FROM " + tableName +
                    " WHERE created > ? ";
            if (untilDateTime != null)
                selectSQL += " AND created < ? ";
            if (set != null)
                selectSQL += " AND manifest->>'dataset' = ? ";

            selectSQL += " LIMIT 10 "; // TEMP
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);

            // Assign parameters
            int parameterIndex = 1;
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
            if (untilDateTime != null)
                preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
            if (set != null)
                preparedStatement.setString(parameterIndex++, set);

            ResultSet resultSet = preparedStatement.executeQuery();

            ResponseCommon.streamResponse(resultSet, request, response);
        }
    }
}

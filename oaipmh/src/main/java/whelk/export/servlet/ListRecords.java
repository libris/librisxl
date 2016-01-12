package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;

public class ListRecords {
    public static void handleListRecordsRequest(HttpServletRequest req, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException
    {
        String from = req.getParameter("from"); // optional
        String until = req.getParameter("until"); // optional
        String set = req.getParameter("set"); // optional
        String resumptionToken = req.getParameter("resumptionToken"); // exclusive, not supported/used
        String metadataPrefix = req.getParameter("metadataPrefix"); // required

        if (resumptionToken != null || metadataPrefix == null)
        {
            OaiPmh.sendOaiPmhError("badArgument", "", response);
            return;
        }

        if (from == null)
            from = "0000-01-01";

        ZonedDateTime fromDateTime = OaiPmh.parseISO8601(from);
        ZonedDateTime untilDateTime = OaiPmh.parseISO8601(until);

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

            OaiPmh.streamResponse(resultSet, response);
        }
    }
}

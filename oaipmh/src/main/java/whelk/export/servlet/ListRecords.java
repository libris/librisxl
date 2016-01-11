package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
            response.sendError(400, "badArgument");

        if (from == null)
            from = "1970-01-01";

        ZonedDateTime fromDateTime = OaiPmh.parseISO8601(from);
        ZonedDateTime untilDateTime = OaiPmh.parseISO8601(until);

        try (Connection dbconn = DataBase.getConnection())
        {
            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
            
            String selectSQL = "SELECT data, manifest FROM " + tableName +
                    " WHERE created > ? LIMIT 5";
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
            preparedStatement.setTimestamp(1, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));

            ResultSet resultSet = preparedStatement.executeQuery();

            OaiPmh.streamResponse(resultSet, response);
        }
    }
}

package whelk.export.servlet;

import whelk.Document;
import whelk.util.LegacyIntegrationTools;

import javax.servlet.http.HttpServletRequest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.StringJoiner;

public class Helpers
{
    public static String getUnknownParameters(HttpServletRequest request, String... knownParameters)
    {
        HashSet<String> knownParametersSet = new HashSet<String>();
        knownParametersSet.addAll(Arrays.asList(knownParameters));

        StringJoiner unknownParameters = new StringJoiner(", ");
        Enumeration parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements())
        {
            String parameterName = (String) parameterNames.nextElement();
            if (!knownParametersSet.contains(parameterName) && !parameterName.equals("verb"))
            {
                unknownParameters.add(parameterName);
            }
        }

        if (unknownParameters.length() == 0)
            return null;
        return unknownParameters.toString();
    }

    public static ZonedDateTime parseISO8601(String dateTimeString)
    {
        if (dateTimeString == null)
            return null;
        if (dateTimeString.length() == 10) // Date only
            dateTimeString += "T00:00:00Z";
        return ZonedDateTime.parse(dateTimeString);
    }

    public static PreparedStatement prepareSameAsStatement(Connection dbconn, String id)
            throws SQLException
    {
        if (id.matches("/http:/[^/].+")) {
            id = id.replace("http:/", "http://");
        } else if (id.matches("/https:/[^/].+")) {
            id = id.replace("https:/", "https://");
        }

        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        String sql = "SELECT id FROM " + tableName + "__identifiers WHERE iri = ?";
        PreparedStatement preparedStatement = dbconn.prepareStatement(sql);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }

    public static PreparedStatement getMatchingDocumentsStatement(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // Construct the query
        String selectSQL = "SELECT data, collection, modified, deleted, " +
                " data#>>'{@graph,1,hasComponent,0,heldBy,0,@id}' AS sigel FROM " +
                tableName;
        if (fromDateTime != null)
            selectSQL += " AND modified >= ? ";
        if (untilDateTime != null)
            selectSQL += " AND modified <= ? ";
        if (setSpec.getRootSet() != null)
            selectSQL += " AND collection = ? ";

        if (setSpec.getSubset() != null)
            selectSQL += " AND data @> ?";

        selectSQL += " ORDER BY modified ";

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setFetchSize(512);

        // Assign parameters
        int parameterIndex = 1;
        if (fromDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
        if (untilDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
        if (setSpec.getRootSet() != null)
            preparedStatement.setString(parameterIndex++, setSpec.getRootSet());
        if (setSpec.getSubset() != null)
        {
            String strMap = "{\"@graph\":[{\"heldBy\":{\"@id\": \""+
                    LegacyIntegrationTools.legacySigelToUri(setSpec.getSubset())+
                    "\"}}]}";

            preparedStatement.setObject(parameterIndex++, strMap, java.sql.Types.OTHER);
        }

        return preparedStatement;
    }
}

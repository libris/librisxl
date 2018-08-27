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
        id = LegacyIntegrationTools.fixUri(id);

        String sql = "SELECT id FROM lddb__identifiers WHERE iri = ?";
        PreparedStatement preparedStatement = dbconn.prepareStatement(sql);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }

    public static PreparedStatement getMatchingDocumentsStatement(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec, String id, boolean includeDependenciesInTimeInterval)
            throws SQLException
    {
        if (setSpec != null && setSpec.getRootSet() != null && setSpec.getRootSet().startsWith(SetSpec.SET_BIB) && setSpec.getSubset() != null)
        {
            // Using a bib:[location] set requires special handling
            return getBibSigelMatchingDocumentsStatement(dbconn, fromDateTime, untilDateTime, setSpec, includeDependenciesInTimeInterval);
        }
        else
        {
            return getNormalMatchingDocumentsStatement(dbconn, fromDateTime, untilDateTime, setSpec, id, includeDependenciesInTimeInterval);
        }
    }

    private static PreparedStatement getBibSigelMatchingDocumentsStatement(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec, boolean includeDependenciesInTimeInterval)
            throws SQLException
    {
        // Construct the query
        String selectSQL = "WITH bib_with_heldby AS (" +
                " SELECT lddb.id, lddb.data, lddb.collection, lddb.modified, lddb.deleted, lddb.changedBy, lddb.data#>>'{@graph,1,heldBy,@id}' AS sigel, lddb.data#>>'{@graph,1,itemOf,@id}' AS itemOf, lddb_attached_holdings.data#>>'{@graph,1,heldBy,@id}' as heldBy" +
                " FROM lddb ";

        selectSQL += " LEFT JOIN lddb lddb_attached_holdings ON lddb.data#>>'{@graph,1,@id}' = lddb_attached_holdings.data#>>'{@graph,1,itemOf,@id}' ";
        selectSQL += " WHERE lddb.collection <> 'definitions' ";

        if (fromDateTime != null)
        {
            if (includeDependenciesInTimeInterval)
                selectSQL += " AND lddb.depMaxModified >= ? ";
            else
                selectSQL += " AND lddb.modified >= ? ";
        }
        if (untilDateTime != null)
        {
            if (includeDependenciesInTimeInterval)
                selectSQL += " AND lddb.depMaxModified <= ? ";
            else
                selectSQL += " AND lddb.modified <= ? ";
        }
        selectSQL += " AND lddb.collection = ? ";
        selectSQL += " ) ";
        selectSQL += " SELECT * FROM bib_with_heldby WHERE heldBy = ?";

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setFetchSize(512);

        // Assign parameters
        int parameterIndex = 1;
        if (fromDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
        if (untilDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
        preparedStatement.setString(parameterIndex++, setSpec.getRootSet());
        preparedStatement.setString(parameterIndex++, LegacyIntegrationTools.legacySigelToUri(setSpec.getSubset()));

        return preparedStatement;
    }

    private static PreparedStatement getNormalMatchingDocumentsStatement(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec, String id, boolean includeDependenciesInTimeInterval)
            throws SQLException
    {
        // Construct the query
        String selectSQL = "SELECT lddb.id, lddb.data, lddb.collection, lddb.modified, lddb.deleted, lddb.changedBy, lddb.data#>>'{@graph,1,heldBy,@id}' AS sigel, lddb.data#>>'{@graph,1,itemOf,@id}' AS itemOf" +
                " FROM lddb ";

        selectSQL += " WHERE lddb.collection <> 'definitions' ";
        if (id != null)
            selectSQL += " AND lddb.id = ? ";
        if (fromDateTime != null)
        {
            if (includeDependenciesInTimeInterval)
                selectSQL += " AND lddb.depMaxModified >= ? ";
            else
                selectSQL += " AND lddb.modified >= ? ";
        }
        if (untilDateTime != null)
        {
            if (includeDependenciesInTimeInterval)
                selectSQL += " AND lddb.depMaxModified <= ? ";
            else
                selectSQL += " AND lddb.modified <= ? ";
        }
        if (setSpec != null)
        {
            if (setSpec.getRootSet() != null)
                selectSQL += " AND lddb.collection = ? ";

            if (setSpec.getSubset() != null) {
                if (setSpec.getRootSet().startsWith(SetSpec.SET_HOLD))
                    selectSQL += " AND lddb.data->'@graph' @> ?";
            }
        }

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setFetchSize(512);

        // Assign parameters
        int parameterIndex = 1;
        if (id != null)
            preparedStatement.setString(parameterIndex++, id);
        if (fromDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L));
        if (untilDateTime != null)
            preparedStatement.setTimestamp(parameterIndex++, new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L));
        if (setSpec != null)
        {
            if (setSpec.getRootSet() != null)
                preparedStatement.setString(parameterIndex++, setSpec.getRootSet());
            if (setSpec.getSubset() != null) {
                if (setSpec.getRootSet().startsWith(SetSpec.SET_HOLD)) {
                    String strMap = "[{\"heldBy\":{\"@id\": \"" +
                            LegacyIntegrationTools.legacySigelToUri(setSpec.getSubset()) +
                            "\"}}]";

                    preparedStatement.setObject(parameterIndex++, strMap, java.sql.Types.OTHER);
                }
            }
        }

        return preparedStatement;
    }
}

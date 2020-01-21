package whelk.export.servlet;

import whelk.Document;
import whelk.util.LegacyIntegrationTools;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

public class Helpers
{
    public static class ResultIterator implements Iterator<Document>, AutoCloseable {

        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private final SetSpec setSpec;
        private final boolean includeDependenciesInTimeInterval;
        private boolean thereIsMore = true;

        public ResultIterator(PreparedStatement statement, SetSpec setSpec, boolean includeDependenciesInTimeInterval)
                throws SQLException {
            this.statement = statement;
            this.resultSet = statement.executeQuery();
            this.setSpec = setSpec;
            this.includeDependenciesInTimeInterval = includeDependenciesInTimeInterval;
        }

        public boolean hasNext()
        {
            try
            {
                if (resultSet.isBeforeFirst())
                    return false;
                return thereIsMore;
            } catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Document next()
        {
            try
            {
                thereIsMore = resultSet.next();
                String data = resultSet.getString("data");
                return new Document(ResponseCommon.mapper.readValue(data, HashMap.class));
            } catch (SQLException | IOException e)
            {
                try
                {
                    statement.cancel();
                } catch (SQLException e2)
                {
                    throw new RuntimeException(e2);
                }
                throw new RuntimeException(e);
            }
        }

        public void close() throws SQLException {
            resultSet.close();
            statement.close();
        }
    }

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

    public static ResultIterator getMatchingDocuments(Connection connection, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec, String id, boolean includeDependenciesInTimeInterval)
            throws SQLException
    {
        PreparedStatement preparedStatement;
        if (id == null)
        {
            String sql = "SELECT id, collection, created, deleted FROM lddb__versions WHERE true";

            if (fromDateTime != null) {
                sql += " AND modified >= ?";
            }
            if (untilDateTime != null) {
                sql += " AND modified <= ?";
            }

            preparedStatement = connection.prepareStatement(sql);
            int parameterIndex = 1;
            if (fromDateTime != null) {
                Timestamp fromTimeStamp = new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L);
                preparedStatement.setTimestamp(parameterIndex++, fromTimeStamp);
            }
            if (untilDateTime != null) {
                Timestamp untilTimeStamp = new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L);
                preparedStatement.setTimestamp(parameterIndex++, untilTimeStamp);
            }
        }
        else
        {
            String sql = "SELECT id, collection, created, deleted FROM lddb WHERE id = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, id);
        }

        return new ResultIterator(preparedStatement, setSpec, includeDependenciesInTimeInterval);
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
        String selectSQL = "SELECT * FROM (" +
                " SELECT lddb.id, lddb.data, lddb.collection, lddb.created, lddb.modified, lddb.deleted, lddb.changedBy, lddb.data#>>'{@graph,1,heldBy,@id}' AS sigel, lddb.data#>>'{@graph,1,itemOf,@id}' AS itemOf, lddb_attached_holdings.data#>>'{@graph,1,heldBy,@id}' as heldBy" +
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
        selectSQL += " ) AS bib_with_heldby ";
        selectSQL += " WHERE heldBy = ?";

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

        System.out.println(preparedStatement);

        return preparedStatement;
    }

    private static PreparedStatement getNormalMatchingDocumentsStatement(Connection dbconn, ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec, String id, boolean includeDependenciesInTimeInterval)
            throws SQLException
    {
        // Construct the query
        String selectSQL = "SELECT lddb.id, lddb.data, lddb.collection, lddb.created, lddb.modified, lddb.deleted, lddb.changedBy, lddb.data#>>'{@graph,1,heldBy,@id}' AS sigel, lddb.data#>>'{@graph,1,itemOf,@id}' AS itemOf" +
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

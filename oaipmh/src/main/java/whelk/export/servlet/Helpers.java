package whelk.export.servlet;

import groovy.lang.Tuple2;
import whelk.Document;
import whelk.JsonLd;
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
        private final Stack<Document> resultingDocuments = new Stack<>();
        private boolean firstAccess = true;

        public ResultIterator(PreparedStatement statement, SetSpec setSpec, boolean includeDependenciesInTimeInterval)
                throws SQLException {
            this.statement = statement;
            this.resultSet = statement.executeQuery();
            this.setSpec = setSpec;
            this.includeDependenciesInTimeInterval = includeDependenciesInTimeInterval;
        }

        private boolean isHeld(Document doc, String bySigel)
        {
            if (bySigel == null)
                return false;
            
            List<Document> holdings = OaiPmh.s_whelk.getAttachedHoldings(doc.getThingIdentifiers());

            for (Document holding : holdings)
            {
                String sigel = holding.getSigel();
                if (bySigel.equals(sigel))
                {
                    return true;
                }
            }
            return false;
        }

        private void emitAffected(Document updated)
        {
            String updatedCollection = LegacyIntegrationTools.determineLegacyCollection(updated, OaiPmh.s_whelk.getJsonld());

            if (updatedCollection == null)
            {
                return;
            }

            if (setSpec == null || setSpec.getRootSet() == null)
            {
                // If no set is used, all records are welcome.
                resultingDocuments.push(updated);
            }
            else if (updatedCollection.equals("auth"))
            {
                if (setSpec.getRootSet().equals("auth"))
                    resultingDocuments.push(updated);
                if (includeDependenciesInTimeInterval && (setSpec.getRootSet().equals("bib") || setSpec.getRootSet().equals("hold")))
                {
                    List<Tuple2<String, String>> dependers = OaiPmh.s_whelk.getStorage().followDependers(updated.getShortId(), JsonLd.getNON_DEPENDANT_RELATIONS());
                    for (Tuple2<String, String> depender : dependers)
                    {
                        String dependerId = depender.getFirst();
                        Document dependerDocument = OaiPmh.s_whelk.getDocument(dependerId);
                        String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDocument, OaiPmh.s_whelk.getJsonld());
                        if (dependerCollection.equals("bib") && setSpec.getRootSet().equals("bib"))
                        {
                            String mustBeHeldBy = setSpec.getSubset();
                            if (mustBeHeldBy == null || isHeld(dependerDocument, mustBeHeldBy))
                                resultingDocuments.push(dependerDocument);
                        }
                        else if (dependerCollection.equals("hold") && setSpec.getRootSet().equals("hold"))
                        {
                            String mustBeHeldBy = setSpec.getSubset();
                            String sigel = dependerDocument.getSigel();
                            if (mustBeHeldBy == null || mustBeHeldBy.equals(sigel))
                            {
                                resultingDocuments.push(updated);
                            }
                        }
                    }
                }
            }
            else if (updatedCollection.equals("bib"))
            {
                if (setSpec.getRootSet().equals("bib"))
                {
                    String mustBeHeldBy = setSpec.getSubset();
                    if (mustBeHeldBy == null || isHeld(updated, mustBeHeldBy))
                    {
                        resultingDocuments.push(updated);
                    }
                }
            }
            else if (updatedCollection.equals("hold"))
            {
                if (setSpec.getRootSet().equals("hold"))
                {
                    String mustBeHeldBy = setSpec.getSubset();
                    String sigel = updated.getSigel();
                    if (mustBeHeldBy == null || mustBeHeldBy.equals(sigel))
                    {
                        resultingDocuments.push(updated);
                    }
                }
            }

        }

        public boolean hasNext()
        {
            try
            {
                if ( firstAccess && (!resultSet.isBeforeFirst()) ) // Empty initial resultset?
                {
                    return false;
                }
                else if (!resultingDocuments.isEmpty()) // Stuff already in the queue?
                {
                    return true;
                }
                else // More to fetch from db?
                {
                    firstAccess = false;
                    while (resultSet.next())
                    {
                        String data = resultSet.getString("data");

                        Document updated = new Document(ResponseCommon.mapper.readValue(data, HashMap.class));
                        emitAffected(updated);

                        // Did reading this record from the DB result in anything new in the export flow?
                        if (!resultingDocuments.isEmpty())
                            return true;
                    }

                    // We've gone over everything that's changed and there is nothing more to export.
                    return false;
                }
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

        public Document next()
        {
            return resultingDocuments.pop();
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
            String sql = "SELECT data FROM lddb WHERE collection <> 'definitions'";

            if (fromDateTime != null) {
                sql += " AND modified >= ?";
            }
            if (untilDateTime != null) {
                sql += " AND modified <= ?";
            }

            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setFetchSize(512);
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
            String sql = "SELECT data FROM lddb WHERE id = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, id);
        }

        return new ResultIterator(preparedStatement, setSpec, includeDependenciesInTimeInterval);
    }
}

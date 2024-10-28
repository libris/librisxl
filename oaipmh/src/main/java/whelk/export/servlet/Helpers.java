package whelk.export.servlet;

import groovy.lang.Tuple2;
import whelk.Document;
import whelk.JsonLd;
import whelk.util.LegacyIntegrationTools;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.StringJoiner;

import static whelk.util.Jackson.mapper;

public class Helpers
{
    public static class ResultIterator implements Iterator<Document>, AutoCloseable {

        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private final String requestedCollection;
        private final String mustBeHeldBy;
        private final String explicitSet;
        private final boolean includeDependenciesInTimeInterval;
        private final Stack<Document> resultingDocuments = new Stack<>();
        private boolean firstAccess = true;

        public ResultIterator(PreparedStatement statement, String requestedCollection, String mustBeHeldBy,
                              boolean includeDependenciesInTimeInterval, String explicitSet)
                throws SQLException {
            this.statement = statement;
            this.resultSet = statement.executeQuery();
            this.requestedCollection = requestedCollection;
            this.mustBeHeldBy = mustBeHeldBy;
            this.includeDependenciesInTimeInterval = includeDependenciesInTimeInterval;
            this.explicitSet = explicitSet;
        }

        private boolean isHeld(Document doc, String bySigel)
        {
            if (bySigel == null)
                return false;
            
            List<Document> holdings = OaiPmh.s_whelk.getAttachedHoldings(doc.getThingIdentifiers());

            for (Document holding : holdings)
            {
                String sigel = holding.getHeldBySigel();
                if (bySigel.equals(sigel))
                {
                    return true;
                }
            }
            return false;
        }

        private void queueDocument(Document doc)
        {
            if (explicitSet == null)
            {
                resultingDocuments.push(doc);
            }
            // Beware: This <marc:..>-stuff is not future proof!
            else if (explicitSet.equals("nb") &&
                    doc.getEncodingLevel() != null &&
                    doc.getEncodingLevel().equals("marc:FullLevel"))
            {
                resultingDocuments.push(doc);
            }
            else if (explicitSet.equals("sao") &&
                    doc.getThingInScheme() != null &&
                    doc.getThingInScheme().equals("https://id.kb.se/term/sao"))
            {
                resultingDocuments.push(doc);
            }
        }

        private void emitAffected(Document updated)
        {
            String updatedCollection = LegacyIntegrationTools.determineLegacyCollection(updated, OaiPmh.s_whelk.getJsonld());

            if (requestedCollection == null)
            {
                // If no collection is requested, all records matching the prepared statement are welcome.
                queueDocument(updated);
            }
            else if (updatedCollection.equals("auth"))
            {
                String type = updated.getThingType();

                if (requestedCollection.equals("auth") &&
                        (type == null || !OaiPmh.workDerivativeTypes.contains(type))) {
                    queueDocument(updated);
                }

                if (includeDependenciesInTimeInterval && (requestedCollection.equals("bib") || requestedCollection.equals("hold")) && hasCardChanged(updated))
                {
                    List<Tuple2<String, String>> dependers = OaiPmh.s_whelk.getStorage().followDependers(updated.getShortId(), JsonLd.NON_DEPENDANT_RELATIONS);
                    for (Tuple2<String, String> depender : dependers)
                    {
                        String dependerId = depender.getV1();
                        Document dependerDocument = OaiPmh.s_whelk.getDocument(dependerId);
                        String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDocument, OaiPmh.s_whelk.getJsonld());
                        if (dependerCollection.equals("bib") && requestedCollection.equals("bib"))
                        {
                            if (mustBeHeldBy == null || isHeld(dependerDocument, mustBeHeldBy))
                                queueDocument(dependerDocument);
                        }
                        else if (dependerCollection.equals("hold") && requestedCollection.equals("hold"))
                        {
                            String sigel = dependerDocument.getHeldBySigel();
                            if (mustBeHeldBy == null || mustBeHeldBy.equals(sigel))
                            {
                                queueDocument(dependerDocument);
                            }
                        }
                    }
                }
            }
            else if (updatedCollection.equals("bib"))
            {
                if (requestedCollection.equals("bib"))
                {
                    if (mustBeHeldBy == null || isHeld(updated, mustBeHeldBy))
                    {
                        queueDocument(updated);
                    }
                }
            }
            else if (updatedCollection.equals("hold"))
            {
                if (requestedCollection.equals("hold"))
                {
                    String sigel = updated.getHeldBySigel();
                    if (mustBeHeldBy == null || mustBeHeldBy.equals(sigel))
                    {
                        queueDocument(updated);
                    }
                }
            }

        }

        private boolean hasCardChanged(Document updated) {
            Document previousVersion = OaiPmh.s_whelk.getStorage().load(updated.getShortId(), "-1");
            if (previousVersion == null) {
                return true;
            }
            
            var jsonLd = OaiPmh.s_whelk.getJsonld();
            var oldCard = jsonLd.toCard(previousVersion.data);
            var newCard = jsonLd.toCard(updated.data);
            return !oldCard.equals(newCard);
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

                        Document updated = new Document(mapper.readValue(data, HashMap.class));
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

    private static PreparedStatement getOpenIntervalStatement(Connection connection, ZonedDateTime fromDateTime,
                                                                ZonedDateTime untilDateTime, boolean includeSilentChanges)
            throws SQLException
    {
        PreparedStatement preparedStatement;
        String sql = "SELECT data FROM lddb WHERE collection in ('bib', 'auth', 'hold')";

        if (fromDateTime != null)
        {
            if (includeSilentChanges)
                sql += " AND ( modified >= ? OR totstz(data#>>'{@graph,0,generationDate}') >= ? ) ";
            else
                sql += " AND modified >= ? ";
        }
        if (untilDateTime != null)
        {
            if (includeSilentChanges)
                sql += " AND ( modified <= ? OR totstz(data#>>'{@graph,0,generationDate}') <= ? ) ";
            else
                sql += " AND modified <= ? ";
        }

        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setFetchSize(512);
        int parameterIndex = 1;
        if (fromDateTime != null)
        {
            Timestamp fromTimeStamp = new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L);
            preparedStatement.setTimestamp(parameterIndex++, fromTimeStamp);
            if (includeSilentChanges)
                preparedStatement.setTimestamp(parameterIndex++, fromTimeStamp);
        }
        if (untilDateTime != null)
        {
            Timestamp untilTimeStamp = new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L);
            preparedStatement.setTimestamp(parameterIndex++, untilTimeStamp);
            if (includeSilentChanges)
                preparedStatement.setTimestamp(parameterIndex++, untilTimeStamp);
        }

        return preparedStatement;
    }

    private static PreparedStatement getClosedIntervalStatement(Connection connection, ZonedDateTime fromDateTime,
                                                      ZonedDateTime untilDateTime, boolean includeSilentChanges)
            throws SQLException
    {
        PreparedStatement preparedStatement;
        String sql = "SELECT data FROM lddb WHERE collection in ('bib', 'auth', 'hold')";

        if (includeSilentChanges)
        {
            sql += " AND ( modified BETWEEN ? AND ? OR totstz(data#>>'{@graph,0,generationDate}') BETWEEN ? AND ? ) ";
        }
        else
        {
            sql += " AND ( modified BETWEEN ? AND ? ) ";
        }

        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setFetchSize(512);

        Timestamp fromTimeStamp = new Timestamp(fromDateTime.toInstant().getEpochSecond() * 1000L);
        Timestamp untilTimeStamp = new Timestamp(untilDateTime.toInstant().getEpochSecond() * 1000L);
        int parameterIndex = 1;
        preparedStatement.setTimestamp(parameterIndex++, fromTimeStamp);
        preparedStatement.setTimestamp(parameterIndex++, untilTimeStamp);
        if (includeSilentChanges)
        {
            preparedStatement.setTimestamp(parameterIndex++, fromTimeStamp);
            preparedStatement.setTimestamp(parameterIndex++, untilTimeStamp);
        }

        return preparedStatement;
    }

    public static ResultIterator getMatchingDocuments(Connection connection, ZonedDateTime fromDateTime,
                                                      ZonedDateTime untilDateTime, SetSpec setSpec, String id,
                                                      boolean includeDependenciesInTimeInterval,
                                                      boolean includeSilentChanges)
            throws SQLException
    {
        PreparedStatement preparedStatement;
        if (id == null)
        {
            if (fromDateTime == null || untilDateTime == null)
                preparedStatement = getOpenIntervalStatement(connection, fromDateTime, untilDateTime, includeSilentChanges);
            else
                preparedStatement = getClosedIntervalStatement(connection, fromDateTime, untilDateTime, includeSilentChanges);
        }
        else
        {
            String sql = "SELECT data FROM lddb WHERE id = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, id);
        }

        // Extract requested marc:collection and explicit set, if any, from setSpec
        String requestedCollection = null;
        String mustBeHeldBy = null;
        String explicitSet = null;
        if (setSpec != null && setSpec.getRootSet() != null)
        {
            switch (setSpec.getRootSet())
            {
                case SetSpec.SET_AUTH:
                case SetSpec.SET_BIB:
                case SetSpec.SET_HOLD:
                    requestedCollection = setSpec.getRootSet();
                    mustBeHeldBy = setSpec.getSubset();
                    break;
                case SetSpec.SET_NB:
                    requestedCollection = SetSpec.SET_BIB;
                    explicitSet = "nb";
                    break;
                case SetSpec.SET_SAO:
                    requestedCollection = SetSpec.SET_AUTH;
                    explicitSet = "sao";
                    break;
            }
        }

        return new ResultIterator(preparedStatement, requestedCollection, mustBeHeldBy,
                includeDependenciesInTimeInterval, explicitSet);
    }
}

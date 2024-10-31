package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static whelk.util.Jackson.mapper;

public class EmmChangeSet {
    private static final int TARGET_HITS_PER_PAGE = 100;
    private static final Logger logger = LogManager.getLogger(EmmChangeSet.class);

    static void sendChangeSet(Whelk whelk, HttpServletResponse res, String category, String until, String ApiBaseUrl) throws IOException {

        List<EmmActivity> activitiesOnPage = new ArrayList<>(TARGET_HITS_PER_PAGE+5);
        Timestamp nextTimeStamp = getPage(whelk, category, until, activitiesOnPage);
        if (nextTimeStamp == null) {
            res.sendError(500);
            return;
        }

        // THIS SHIT is so painful in Java :(
        HashMap responseObject = new HashMap();
        ArrayList contexts = new ArrayList();
        contexts.add("https://www.w3.org/ns/activitystreams");
        contexts.add("https://emm-spec.org/1.0/context.json");
        responseObject.put("@context", contexts);
        responseObject.put("type", "OrderedCollectionPage");
        responseObject.put("id", ApiBaseUrl+"?category="+category+"&until="+until);
        HashMap partOf = new HashMap();
        partOf.put("type", "OrderedCollectionPage");
        partOf.put("id", "TODO");
        responseObject.put("partOf", partOf);
        responseObject.put("next", ApiBaseUrl+"?category="+category+"&until="+nextTimeStamp.getTime());
        List orderedItems = new ArrayList();
        responseObject.put("orderedItems", orderedItems);

        for (EmmActivity activityInList : activitiesOnPage) {
            HashMap activityInStream = new HashMap();
            activityInStream.put("type", switch (activityInList.activityType) {
                case CREATE -> "create";
                case UPDATE -> "update";
                case DELETE -> "delete";
            });
            activityInStream.put("published", ZonedDateTime.ofInstant(activityInList.modificationTime.toInstant(), ZoneOffset.UTC).toString());
            HashMap activityObject = new HashMap();
            activityInStream.put("object", activityObject);
            activityObject.put("id", activityInList.uri);
            activityObject.put("type", activityInList.entityType);
            activityObject.put("updated", ZonedDateTime.ofInstant(activityInList.modificationTime.toInstant(), ZoneOffset.UTC).toString());
            orderedItems.add(activityInStream);
        }

        String jsonResponse = mapper.writeValueAsString(responseObject);
        BufferedWriter writer = new BufferedWriter( res.getWriter() );
        writer.write(jsonResponse);
        writer.close();
    }

    /**
     * Get a page's worth of items. The results will be added to the 'result'-list. The return value will be "the next timestamp"
     * to start getting the next page at, or null on failure.
     */
    private static Timestamp getPage(Whelk whelk, String category, String until, List<EmmActivity> result) {

        // Internally 'until' is "milliseconds since epoch".
        long untilNumerical = Long.parseLong(until);
        Timestamp untilTimeStamp = new Timestamp(untilNumerical);

        Timestamp earliestSeenTimeStamp = new Timestamp(System.currentTimeMillis()); // This is (will be set to) how far back in time this page stretches

        try (Connection connection = whelk.getStorage().getOuterConnection()) {

            // Get a page of items
            {
                String sql = "SELECT" +
                        "  data#>>'{@graph,1,@id}'," +
                        "  GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}'))," +
                        "  deleted," +
                        "  created," +
                        "  data#>>'{@graph,1,@type}'" +
                        " FROM" +
                        "  lddb__versions" +
                        " WHERE GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) <= ? " +
                        " ORDER BY GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) DESC LIMIT ? ".stripIndent();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                preparedStatement.setInt(2, TARGET_HITS_PER_PAGE);
                preparedStatement.setFetchSize(TARGET_HITS_PER_PAGE + 1);
                System.err.println(preparedStatement);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);
                        Timestamp modificationTime = resultSet.getTimestamp(2);
                        boolean deleted = resultSet.getBoolean(3);
                        Timestamp creationTime = resultSet.getTimestamp(4);
                        String type = resultSet.getString(5);
                        result.add(new EmmActivity(uri, type, creationTime, modificationTime, deleted));
                        if (modificationTime.before(earliestSeenTimeStamp))
                            earliestSeenTimeStamp = modificationTime;
                    }
                }
            }

            // Get any extra records that share an exact modification time with the earliest seen time on this page
            {
                String sql = "SELECT" +
                        "  data#>>'{@graph,1,@id}'," +
                        "  GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}'))," +
                        "  deleted," +
                        "  created," +
                        "  data#>>'{@graph,1,@type}'" +
                        " FROM lddb__versions WHERE GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) = ?".stripIndent();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);
                        Timestamp modificationTime = resultSet.getTimestamp(2);
                        boolean deleted = resultSet.getBoolean(3);
                        Timestamp creationTime = resultSet.getTimestamp(4);
                        String type = resultSet.getString(5);
                        result.add(new EmmActivity(uri, type, creationTime, modificationTime, deleted));
                    }
                }
            }
        } catch (SQLException se) {
            logger.error(se);
            se.printStackTrace();
            return null;
        }

        return earliestSeenTimeStamp;
    }
}

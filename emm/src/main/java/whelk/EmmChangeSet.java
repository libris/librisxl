package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.util.http.HttpTools;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static whelk.EmmServlet.AS2_CONTENT_TYPE;

public class EmmChangeSet {
    public static final int TARGET_HITS_PER_PAGE = 100;
    private static final Logger logger = LogManager.getLogger(EmmChangeSet.class);

    static void sendChangeSet(Whelk whelk, HttpServletResponse res, String until, String apiBaseUrl) throws IOException {

        var ld = whelk.getJsonld();

        List<EmmActivity> activitiesOnPage = new ArrayList<>(TARGET_HITS_PER_PAGE+5);
        Timestamp nextTimeStamp = getPage(whelk, until, activitiesOnPage);
        if (nextTimeStamp == null) {
            res.sendError(500);
            return;
        }

        var responseObject = new LinkedHashMap<>();

        var contexts = new ArrayList<>();
        contexts.add("https://www.w3.org/ns/activitystreams");
        contexts.add("https://emm-spec.org/1.0/context.json");
        var ctx = new HashMap<String,String>();
        ctx.put(ld.getVocabPrefix(), ld.getVocabId());
        prefixes(activitiesOnPage).forEach(prefix -> ctx.put(prefix, ld.getNamespaceUri(prefix)));
        contexts.add(ctx);
        responseObject.put("@context", contexts);

        responseObject.put("type", "OrderedCollectionPage");
        responseObject.put("id", apiBaseUrl+"?until="+until);
        var partOf = new LinkedHashMap<>();
        partOf.put("type", "OrderedCollection");
        partOf.put("id", apiBaseUrl);
        responseObject.put("partOf", partOf);
        responseObject.put("next", apiBaseUrl+"?until="+nextTimeStamp.getTime());
        var orderedItems = new ArrayList<>();
        responseObject.put("orderedItems", orderedItems);

        for (EmmActivity activityInList : activitiesOnPage) {
            var activityInStream = new LinkedHashMap<>();
            activityInStream.put("type", switch (activityInList.activityType) {
                case CREATE -> "Create";
                case UPDATE -> "Update";
                case DELETE -> "Delete";
            });
            activityInStream.put("published", ZonedDateTime.ofInstant(activityInList.modificationTime.toInstant(), ZoneOffset.UTC).toString());
            var activityObject = new HashMap<>();
            activityInStream.put("object", activityObject);
            activityObject.put("id", activityInList.uri);
            if (activityInList.entityType != null)
                activityObject.put("type", ld.prependVocabPrefix(activityInList.entityType));
            if (activityInList.library != null) {
                activityObject.put("kbv:heldBy", Map.of("@id", activityInList.library));
            }
            activityObject.put("updated", ZonedDateTime.ofInstant(activityInList.modificationTime.toInstant(), ZoneOffset.UTC).toString());
            orderedItems.add(activityInStream);
        }

        HttpTools.sendResponse(res, responseObject, AS2_CONTENT_TYPE);
    }

    /**
     * Get a page's worth of items. The results will be added to the 'result'-list. The return value will be "the next timestamp"
     * to start getting the next page at, or null on failure.
     */
    private static Timestamp getPage(Whelk whelk, String until, List<EmmActivity> result) {

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
                        "  data#>>'{@graph,1,@type}', " +
                        "  data#>>'{@graph,1,heldBy,@id}'" + // In case of holding records, also get which library it is for
                        " FROM" +
                        "  lddb__versions" +
                        " WHERE GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) <= ? " +
                        " ORDER BY GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) DESC LIMIT ? ";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                preparedStatement.setInt(2, TARGET_HITS_PER_PAGE);
                preparedStatement.setFetchSize(TARGET_HITS_PER_PAGE + 1);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);
                        Timestamp modificationTime = resultSet.getTimestamp(2);
                        boolean deleted = resultSet.getBoolean(3);
                        Timestamp creationTime = resultSet.getTimestamp(4);
                        String type = resultSet.getString(5);
                        String library = resultSet.getString(6);
                        if (type != null && uri != null && modificationTime != null)
                            result.add(new EmmActivity(uri, type, creationTime, modificationTime, deleted, library));
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
                        "  data#>>'{@graph,1,@type}', " +
                        "  data#>>'{@graph,1,heldBy,@id}'" + // In case of holding records, also get which library it is for
                        " FROM lddb__versions WHERE GREATEST(modified, totstz(data#>>'{@graph,0,generationDate}')) = ?";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);
                        Timestamp modificationTime = resultSet.getTimestamp(2);
                        boolean deleted = resultSet.getBoolean(3);
                        Timestamp creationTime = resultSet.getTimestamp(4);
                        String type = resultSet.getString(5);
                        String library = resultSet.getString(6);
                        result.add(new EmmActivity(uri, type, creationTime, modificationTime, deleted, library));
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

    static Set<String> prefixes(Collection<EmmActivity> activities) {
        return activities.stream()
                .map(a -> a.entityType)
                .filter(Objects::nonNull)
                .map(type -> type.split(":"))
                .filter(a -> a.length == 2)
                .map(a -> a[0])
                .collect(Collectors.toSet());
    }
}

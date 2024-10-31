package whelk;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class EmmServlet extends HttpServlet {
    private static final int TARGET_HITS_PER_PAGE = 100;
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final HashSet<String> availableCategories;
    private final Whelk whelk;
    public EmmServlet() {
        availableCategories = new HashSet<>();
        availableCategories.add("all");
        whelk = Whelk.createLoadedCoreWhelk();
    }

    public void init() {
    }

    public void destroy() {
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {
            logger.info("Received request with from {}:{}.", req.getRemoteAddr(), req.getRemotePort());

            String category = req.getParameter("category");
            String until = req.getParameter("until");

            System.err.println("category: " + category);
            System.err.println("until: " + until);

            if (!availableCategories.contains(category)) {
                res.sendError(400); // temp
                return;
            }

            if (until == null) { // Send EntryPoint reply
                res.sendError(400); // temp
                return;
            }

            // Send ChangeSet reply
            sendChangeSet(whelk, res, category, until);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendChangeSet(Whelk whelk, HttpServletResponse res, String category, String until) throws IOException {

        List<EmmActivity> activitiesOnPage = new ArrayList<>(TARGET_HITS_PER_PAGE+5);
        Timestamp nextTimeStamp = getPage(whelk, category, until, activitiesOnPage);
        if (nextTimeStamp == null) {
            res.sendError(500);
            return;
        }

        for (EmmActivity a : activitiesOnPage) {
            System.err.println(a);
        }
    }

    /**
     * Get a page's worth of items. The results will be added to the 'result'-list. The return value will be "the next timestamp"
     * to start getting the next page at, or null on failure.
     */
    private Timestamp getPage(Whelk whelk, String category, String until, List<EmmActivity> result) {

        // Internally 'until' is "milliseconds since epoch".
        long untilNumerical = Long.parseLong(until);
        Timestamp untilTimeStamp = new Timestamp(untilNumerical);

        Timestamp earliestSeenTimeStamp = new Timestamp(System.currentTimeMillis()); // This is (will be set to) how far back in time this page stretches

        try (Connection connection = whelk.getStorage().getOuterConnection()) {

            // Get a page of items
            {
                String sql = "SELECT" +
                                "  data#>>'{@graph,1,@id}'," +
                                "  GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz)," +
                                "  deleted," +
                                "  created," +
                                "  data#>>'{@graph,1,@type}'" +
                                " FROM" +
                                "  lddb__versions" +
                                " WHERE GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) <= ? " +
                                " ORDER BY GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) DESC LIMIT ? ".stripIndent();
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
                        "  deleted," +
                        "  created," +
                        "  data#>>'{@graph,1,@type}'" +
                        " FROM lddb__versions WHERE GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) = ?".stripIndent();
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
            return null;
        }

        return earliestSeenTimeStamp;
    }
}

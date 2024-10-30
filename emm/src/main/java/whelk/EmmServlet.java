package whelk;

import whelk.Whelk;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class EmmServlet extends HttpServlet {

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
        final int targetHitsPerPage = 100;

        // Internally 'until' is "milliseconds since epoch".
        long untilNumerical = Long.parseLong(until);
        Timestamp untilTimeStamp = new Timestamp(untilNumerical);

        Timestamp earliestSeenTimeStamp = new Timestamp(System.currentTimeMillis()); // This is how far back in time this page stretches

        try (Connection connection = whelk.getStorage().getOuterConnection()) {

            // Get a page of items
            {
                String sql =
                        "SELECT data#>>'{@graph,0,@id}', GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) " +
                                "  FROM lddb WHERE GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) <= ? " +
                                "  ORDER BY GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) DESC LIMIT ? ".stripIndent();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                preparedStatement.setInt(2, targetHitsPerPage);
                preparedStatement.setFetchSize(targetHitsPerPage + 1);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);
                        Timestamp modificationTime = resultSet.getTimestamp(2);

                        System.err.println("at " + modificationTime + " -> " + uri);

                        if (modificationTime.before(earliestSeenTimeStamp))
                            earliestSeenTimeStamp = modificationTime;
                    }
                }
            }

            // Get any extra records that share an exact modification time with the earliest seen time on this page (above)
            {
                String sql = "SELECT data#>>'{@graph,0,@id}' FROM lddb WHERE GREATEST(modified, (data#>>'{@graph,0,generationDate}')::timestamptz) = ?";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setTimestamp(1, untilTimeStamp);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String uri = resultSet.getString(1);

                        System.err.println("at same time! " + uri);
                    }
                }
            }

            System.err.println("Starting point for next page: " + earliestSeenTimeStamp);

        } catch (SQLException se) {
            res.sendError(500);
        }
    }
}

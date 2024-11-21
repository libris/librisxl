package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.util.http.HttpTools;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Dump {
    /* Here is how these dumps work:
     * When someone asks for a dump of particular category, we check if we have one already.
     * If we do not, we start generating one and delay our response a little bit, until enough
     * of the new dump has been generated to fill the first page.
     *
     * The generated dumps on disks are text files containing only XL system IDs of the records
     * that make up the dump. The actual data for each record is not stored in these files, but
     * only added into the response stream at the last moment.
     *
     * The dumps on disk have a rather particular format. They consist of a number of lines,
     * that are all exactly 17 bytes long, with one ID on each line. The number 17 is chosen
     * merely because it is the smallest number that can hold any internal libris ID followed
     * by a line break, but there is another reason why *all* lines must be exactly 17 bytes
     * (even though some IDs are shorter). The point of this, is to be able to serve any part
     * (page) of the response, without having the scan through the whole dump. If someone asks
     * for a page starting att offset one million, we can simply start reading the dump file
     * at byte offset 17 million, and not have to read through the millions of bytes before that.
     *
     * The last line of a finished dump holds a (also 17 bytes) marker, to separate the finished
     * dump from one that is still being generated but just haven't gotten any further yet.
     */
    private static final Logger logger = LogManager.getLogger(Dump.class);
    private static final String DUMP_END_MARKER = "_DUMP_END_MARKER\n"; // Must be 17 bytes
    private static final String JSON_CONTENT_TYPE = "application/json";

    public static void sendDumpResponse(Whelk whelk, String apiBaseUrl, HttpServletRequest req, HttpServletResponse res) throws IOException, SQLException {
        String dump = req.getParameter("dump");

        if (dump.equals("index")) {
            sendDumpIndexResponse(apiBaseUrl, res);
            return;
        }

        String offset = req.getParameter("offset");
        long offsetNumeric = Long.parseLong(offset);

        String tmpDir = System.getProperty("java.io.tmpdir");
        Path dumpsPath = Paths.get(tmpDir, "dumps");
        Files.createDirectories(dumpsPath);
        Path dumpFilePath = dumpsPath.resolve(dump+".dump");

        invalidateIfOld(dumpFilePath);
        if (!Files.exists(dumpFilePath))
            generateDump(whelk, dump, dumpFilePath);
        sendDumpPageResponse(whelk, apiBaseUrl, dump, dumpFilePath, offsetNumeric, res);
    }

    private static void sendDumpIndexResponse(String apiBaseUrl, HttpServletResponse res) throws IOException {
        var responseObject = new LinkedHashMap<>();

        var categoriesList = new ArrayList<>();

        var allCategory = new LinkedHashMap<>();
        allCategory.put("url", apiBaseUrl+"?dump=all&offset=0");
        allCategory.put("description", "This category represents the whole collection, without reservations.");
        categoriesList.add(allCategory);

        var libraryCategory = new LinkedHashMap<>();
        libraryCategory.put("url", apiBaseUrl+"?dump=itemAndInstance:X&offset=0");
        libraryCategory.put("description", "These categories represent the Items and Instances held by a particular library. " +
                "The relevant library-code (sigel) for which you want data must replace the X in the category URL.");
        categoriesList.add(libraryCategory);

        var typesCategory = new LinkedHashMap<>();
        typesCategory.put("url", apiBaseUrl+"?dump=type:X&offset=0");
        typesCategory.put("description", "These categories represent the set of entities of a certain type, including subtypes. " +
                "For example the type Agent would include both Persons and Organizations etc. The X in the URL must be replaced " +
                "with the type you want.");
        categoriesList.add(typesCategory);

        responseObject.put("categories", categoriesList);
        responseObject.put("warning", "This description of the available dump categories is a temporary one which will NOT look like this for long. Be careful not to rely on the format or even existence of this particular page.");

        HttpTools.sendResponse(res, responseObject, JSON_CONTENT_TYPE);
    }

    private static void sendDumpPageResponse(Whelk whelk, String apiBaseUrl, String dump, Path dumpFilePath, long offsetLines, HttpServletResponse res) throws IOException {
        ArrayList<String> recordIdsOnPage = new ArrayList<>(EmmChangeSet.TARGET_HITS_PER_PAGE);
        Long totalEntityCount = null;

        try {
            // Has the dump not begun being written yet ?
            var t = new Timeout(60 * 1000);
            while (!Files.exists(dumpFilePath)) {
                t.sleep();
            }

            try (RandomAccessFile file = new RandomAccessFile(dumpFilePath.toFile(), "r")) {
                byte[] lineBuffer = new byte[17];

                // Is the dump generation finished ?
                boolean dumpFinished = false;
                if (file.length() >= 17) {
                    file.seek(file.length() - 17);
                    dumpFinished = ( 17 == file.read(lineBuffer) && new String(lineBuffer, StandardCharsets.UTF_8).equals(DUMP_END_MARKER) );
                }

                // Is there not enough data for a full page yet ?
                long offsetBytes = 17 * offsetLines;
                t = new Timeout(60 * 1000);
                while (!dumpFinished && file.length() < offsetBytes + (17 * (long)EmmChangeSet.TARGET_HITS_PER_PAGE)) {
                    t.sleep();

                    if (file.length() >= 17) {
                        file.seek(file.length() - 17);
                        dumpFinished = ( 17 == file.read(lineBuffer) && new String(lineBuffer, StandardCharsets.UTF_8).equals(DUMP_END_MARKER) );
                    }
                }

                if (dumpFinished) {
                    totalEntityCount = file.length() / 17 - 1;
                }

                // We're ok to send a full page, or the end of the dump (which may be a partial page).
                file.seek(offsetBytes);
                int recordsToSend = Integer.min(EmmChangeSet.TARGET_HITS_PER_PAGE, (int)((file.length() - offsetBytes) / 17) - 1);
                for (int i = 0; i < recordsToSend; ++i) {
                    if (17 == file.read(lineBuffer)) {
                        recordIdsOnPage.add(new String(lineBuffer, StandardCharsets.UTF_8).trim());
                    } else {
                        logger.error("Suspected corrupt dump (non-17-byte line detected): {}", dumpFilePath);
                        HttpTools.sendError(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "");
                        return;
                    }
                }

            }
        } catch (Timeout.TimeOutException | InterruptedException e) {
            logger.info("Timed out (1 minute) waiting for enough data to be generated in: {}", dumpFilePath);
            HttpTools.sendError(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "timeout");
            return;
        } catch (IOException e) {
            logger.error("Failed reading dumpfile: {}", dumpFilePath, e);
            HttpTools.sendError(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "");
            return;
        }

        BasicFileAttributes attributes = Files.readAttributes(dumpFilePath, BasicFileAttributes.class);
        Instant dumpCreationTime = attributes.creationTime().toInstant();
        sendFormattedResponse(whelk, apiBaseUrl, dump, recordIdsOnPage, res, offsetLines + EmmChangeSet.TARGET_HITS_PER_PAGE, totalEntityCount, dumpCreationTime);
    }

    private static void sendFormattedResponse(Whelk whelk, String apiBaseUrl, String dump, ArrayList<String> recordIdsOnPage, HttpServletResponse res, long nextLineOffset, Long totalEntityCount, Instant dumpCreationTime) throws IOException{
        var responseObject = new LinkedHashMap<>();

        responseObject.put("creationTime", ZonedDateTime.ofInstant(dumpCreationTime, ZoneOffset.UTC).toString());
        if (totalEntityCount == null)
            responseObject.put("status", "generating");
        else {
            responseObject.put("status", "done");
            responseObject.put("totalEntityCount", totalEntityCount);
        }

        if (totalEntityCount == null || nextLineOffset < totalEntityCount) {
            responseObject.put("next", apiBaseUrl+"?dump="+dump+"&offset="+nextLineOffset);
        }

        var entitiesList = new ArrayList<>(EmmChangeSet.TARGET_HITS_PER_PAGE);
        responseObject.put("entities", entitiesList);
        Map<String, Document> idsAndRecords = whelk.bulkLoad(recordIdsOnPage);
        for (Document doc : idsAndRecords.values()) {

            // Here is a bit of SPECIALIZED treatment only for the itemAndInstance:categories. These should
            // include not only the Item (which is the root node for this category), but also the linked Instance.
            // Without this, a client must individually GET every single Instance in their dataset, which scales poorly.
            if (dump.startsWith("itemAndInstance:")) {
                String itemOf = doc.getHoldingFor();
                if (itemOf == null) {
                    logger.warn("Holding of nothing? " + doc.getId());
                    continue;
                }
                Document instance = new Document( whelk.loadData(itemOf) );
                if (instance == null) {
                    logger.warn("Bad instance? " + itemOf);
                    continue;
                }
                var itemOfPath = new ArrayList<>();
                itemOfPath.add("@graph"); itemOfPath.add(1); itemOfPath.add("itemOf"); // unggh..
                doc._set(itemOfPath, instance.getThing(), doc.data);
                entitiesList.add(doc.getThing());
            }

            // For normal categories
            else {
                entitiesList.add(doc.getThing());
            }

        }

        HttpTools.sendResponse(res, responseObject, JSON_CONTENT_TYPE);
    }

    private static void invalidateIfOld(Path dumpFilePath) {
        try {
            if (!Files.exists(dumpFilePath))
                return;

            BasicFileAttributes attributes = Files.readAttributes(dumpFilePath, BasicFileAttributes.class);
            if (attributes.creationTime().toInstant().isBefore(Instant.now().minus(5, ChronoUnit.DAYS))) {
                Files.delete(dumpFilePath);
            }
        } catch (IOException e) {
            // These exceptions are caught here due to the (theoretical) risk of file access race conditions.
            // For example, it could be that a dump is being read by one thread, while passing the too-old-threshold
            // and then while still being read, another thread sees the dump as too old and tries to delete it.
            // Just log this sort of thing and carry on.
            logger.info("Failed to invalidate (delete) EMM dump: " + dumpFilePath, e);
        }
    }

    private static void generateDump(Whelk whelk, String dump, Path dumpFilePath) {
        new Thread(() -> {

            // Guard against two racing threads trying to generate the same dump.
            // createNewFile atomically checks if the file exists.
            try {
                if (!dumpFilePath.toFile().createNewFile())
                    return;
            } catch (IOException e) {
                return;
            }

            try (BufferedWriter dumpFileWriter = new BufferedWriter(new FileWriter(dumpFilePath.toFile()));
                 Connection connection = whelk.getStorage().getOuterConnection()) {
                connection.setAutoCommit(false);

                PreparedStatement preparedStatement = null;

                if (dump.equals("all")) {
                    preparedStatement = getAllDumpStatement(connection);
                } else if (dump.startsWith("itemAndInstance:")) {
                    preparedStatement = getLibraryXDumpStatement(connection, dump.substring(16));
                } else if (dump.startsWith("type:")) {
                    preparedStatement = getTypeXDumpStatement(connection, whelk, dump.substring(5));
                }

                if (preparedStatement == null) {
                    logger.info("Dump request for unknown category: " + dump);
                    return;
                }

                try (PreparedStatement p = preparedStatement) {
                    p.setFetchSize(EmmChangeSet.TARGET_HITS_PER_PAGE);
                    try (ResultSet resultSet = p.executeQuery()) {
                        while (resultSet.next()) {

                            // Each line must be exactly 17 bytes long, including the (unix) line break.
                            String id = String.format("%-16s\n", resultSet.getString(1));
                            dumpFileWriter.write(id);
                        }
                    }
                }

                dumpFileWriter.write( String.format("%-17s", DUMP_END_MARKER) );
            } catch (IOException | SQLException e) {
                logger.error("Failed dump generation", e);
                dumpFilePath.toFile().delete();
            }
        }).start();
    }

    private static PreparedStatement getAllDumpStatement(Connection connection) throws SQLException {
        String sql = " SELECT " +
                "  id" +
                " FROM" +
                "  lddb";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return preparedStatement;
    }

    private static PreparedStatement getLibraryXDumpStatement(Connection connection, String library) throws SQLException {
        String sql = " SELECT " +
                "  id" +
                " FROM" +
                "  lddb" +
                " WHERE" +
                "  collection = 'hold' AND" +
                "  (data#>>'{@graph,1,heldBy,@id}' = ? OR data#>>'{@graph,1,heldBy,@id}' = ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatement.setString(1, Document.getBASE_URI().resolve("/library/"+library).toString());

        // This is uncomfortable. Library URIs are "https://libris.kb.se/library/.." regardless of environment base-URI.
        // To protect ourselves from the fact that this could change, check both these URIs and environment-specific ones.
        URI defaultLibrisURI = null;
        try { defaultLibrisURI = new URI("https://libris.kb.se"); } catch (URISyntaxException e) { /*ignore*/ }
        preparedStatement.setString(2, defaultLibrisURI.resolve("/library/"+library).toString());

        return preparedStatement;
    }

    private static PreparedStatement getTypeXDumpStatement(Connection connection, Whelk whelk, String type) throws SQLException {
        Set<String> types = whelk.getJsonld().getSubClasses(type);
        types.add(type);

        String sql = " SELECT " +
                "  id" +
                " FROM" +
                "  lddb" +
                " WHERE" +
                "  data#>>'{@graph,1,@type}' = ANY( ? )";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setArray(1, connection.createArrayOf("TEXT",  types.toArray() ));

        return preparedStatement;
    }

    private static class Timeout {
        long time;
        Timeout(long maxWaitMs) {
            this.time = System.currentTimeMillis() + maxWaitMs;
        }

        public void sleep() throws InterruptedException, TimeOutException {
            Thread.sleep(10);
            if (System.currentTimeMillis() > time) {
                throw new TimeOutException();
            }
        }

        static class TimeOutException extends Exception {}
    }
}

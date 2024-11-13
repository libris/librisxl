package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static whelk.util.Jackson.mapper;

public class Dump {
    private static final Logger logger = LogManager.getLogger(Dump.class);
    private static final String DUMP_END_MARKER = "_DUMP_END_MARKER\n"; // Must be 17 bytes

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
        HashMap responseObject = new HashMap();

        ArrayList<Map> categoriesList = new ArrayList<>();

        HashMap allCategory = new HashMap();
        allCategory.put("url", apiBaseUrl+"?dump=all&offset=0");
        allCategory.put("description", "This category represents the whole collection, without reservations.");
        categoriesList.add(allCategory);

        HashMap libraryCategory = new HashMap();
        libraryCategory.put("url", apiBaseUrl+"?dump=itemAndInstance-X&offset=0");
        libraryCategory.put("description", "These categories represent the Items and Instances held by a particular library. " +
                "The relevant library-code (sigel) for which you want data must replace the X in the category URL.");
        categoriesList.add(libraryCategory);

        HashMap typesCategory = new HashMap();
        typesCategory.put("url", apiBaseUrl+"?dump=type:X&offset=0");
        typesCategory.put("description", "These categories represent the set of a entities of a certain type, including subtypes. " +
                "For example the type Agent would include both Persons and Organizations etc. The X in the URL must be replaced " +
                "with the type you want.");
        categoriesList.add(typesCategory);

        responseObject.put("categories", categoriesList);

        String jsonResponse = mapper.writeValueAsString(responseObject);
        BufferedWriter writer = new BufferedWriter( res.getWriter() );
        writer.write(jsonResponse);
        writer.close();
    }

    private static void sendDumpPageResponse(Whelk whelk, String apiBaseUrl, String dump, Path dumpFilePath, long offsetLines, HttpServletResponse res) throws IOException {
        ArrayList<String> recordIdsOnPage = new ArrayList<>(EmmChangeSet.TARGET_HITS_PER_PAGE);
        Long totalEntityCount = null;

        try {
            // Has the dump not begun being written yet ?
            while (!Files.exists(dumpFilePath)) {
                Thread.sleep(10);
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
                while (!dumpFinished && file.length() < offsetBytes + (17 * (long)EmmChangeSet.TARGET_HITS_PER_PAGE)) {
                    Thread.sleep(10);

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
                        logger.error("Suspected corrupt dump (non-17-byte line detected): " + dumpFilePath);
                        res.sendError(500);
                        return;
                    }
                }

            }
        } catch (IOException | InterruptedException e) {
            logger.error("Failed reading dumpfile: " + dumpFilePath, e);
        }

        sendFormattedResponse(whelk, apiBaseUrl, dump, recordIdsOnPage, res, offsetLines + EmmChangeSet.TARGET_HITS_PER_PAGE, totalEntityCount);
    }

    private static void sendFormattedResponse(Whelk whelk, String apiBaseUrl, String dump, ArrayList<String> recordIdsOnPage, HttpServletResponse res, long nextLineOffset, Long totalEntityCount) throws IOException{
        HashMap responseObject = new HashMap();

        if (totalEntityCount == null)
            responseObject.put("status", "generating");
        else {
            responseObject.put("status", "done");
            responseObject.put("totalEntityCount", totalEntityCount);
        }

        if (totalEntityCount == null || nextLineOffset < totalEntityCount) {
            responseObject.put("next", apiBaseUrl+"?dump="+dump+"&offset="+nextLineOffset);
        }

        ArrayList<Map> entitesList = new ArrayList<>(EmmChangeSet.TARGET_HITS_PER_PAGE);
        responseObject.put("entities", entitesList);
        Map<String, Document> idsAndRecords = whelk.bulkLoad(recordIdsOnPage);
        for (Document doc : idsAndRecords.values()) {

            // Here is a bit of SPECIALIZED treatment only for the itemAndInstance-categories. These should
            // include not only the Item (which is the root node for this category), but also the linked Instance.
            // Without this, a client must individually GET every single Instance in their dataset, which scales poorly.
            if (dump.startsWith("itemAndInstance-")) {
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
                ArrayList itemOfPath = new ArrayList();
                itemOfPath.add("@graph"); itemOfPath.add(1); itemOfPath.add("itemOf"); // unggh..
                doc._set(itemOfPath, instance.getThing(), doc.data);
                entitesList.add(doc.getThing());
            }

            // For normal categories
            else {
                entitesList.add(doc.getThing());
            }

        }

        String jsonResponse = mapper.writeValueAsString(responseObject);
        BufferedWriter writer = new BufferedWriter( res.getWriter() );
        writer.write(jsonResponse);
        writer.close();
    }

    private static void invalidateIfOld(Path dumpFilePath) {
        // TODO
    }

    private static void generateDump(Whelk whelk, String dump, Path dumpFilePath) {
        new Thread(() -> {
            try (BufferedWriter dumpFileWriter = new BufferedWriter(new FileWriter(dumpFilePath.toFile()));
                 Connection connection = whelk.getStorage().getOuterConnection()) {
                connection.setAutoCommit(false);

                PreparedStatement preparedStatement = null;

                if (dump.equals("all")) {
                    preparedStatement = getAllDumpStatement(connection);
                } else if (dump.startsWith("itemAndInstance-")) {
                    preparedStatement = getLibraryXDumpStatement(connection, dump.substring(16));
                } else if (dump.startsWith("type:")) {
                    preparedStatement = getTypeXStatement(connection, whelk, dump.substring(5));
                }

                if (preparedStatement == null) {
                    logger.info("Dump request for unknown category: " + dump);
                    return;
                }

                try (PreparedStatement p = preparedStatement) {
                    p.setFetchSize(EmmChangeSet.TARGET_HITS_PER_PAGE);
                    try (ResultSet resultSet = p.executeQuery()) {
                        while (resultSet.next()) {

                            // Each line must be exactly 17 bytes long, including the (unix-)line break.
                            String id = String.format("%-16s\n", resultSet.getString(1));
                            dumpFileWriter.write(id);
                        }
                    }
                }

                dumpFileWriter.write( String.format("%-17s", DUMP_END_MARKER) );
            } catch (IOException | SQLException e) {
                logger.error("Failed dump generation", e);
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

    private static PreparedStatement getTypeXStatement(Connection connection, Whelk whelk, String type) throws SQLException {
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
}

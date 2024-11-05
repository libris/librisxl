package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static whelk.util.Jackson.mapper;

public class Dump {
    private static final Logger logger = LogManager.getLogger(Dump.class);
    private static final String DUMP_END_MARKER = "_DUMP_END_MARKER\n"; // Must be 17 bytes

    public static void sendDumpResponse(Whelk whelk, String apiBaseUrl, HttpServletRequest req, HttpServletResponse res) throws IOException, SQLException {
        String dump = req.getParameter("dump");
        String offset = req.getParameter("offset");
        long offsetNumeric = Long.parseLong(offset);

        String tmpDir = System.getProperty("java.io.tmpdir");
        Path dumpsPath = Paths.get(tmpDir, "dumps");
        Files.createDirectories(dumpsPath);
        Path dumpFilePath = dumpsPath.resolve(dump+".dump");

        invalidateIfOld(dumpFilePath);
        if (!Files.exists(dumpFilePath))
            generateDump(whelk, dumpFilePath);
        sendDumpPageResponse(whelk, apiBaseUrl, dump, dumpFilePath, offsetNumeric, res);
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

        if (nextLineOffset < totalEntityCount) {
            responseObject.put("next", apiBaseUrl+"?dump="+dump+"&offset="+nextLineOffset);
        }

        ArrayList<Map> entitesList = new ArrayList<>(EmmChangeSet.TARGET_HITS_PER_PAGE);
        responseObject.put("entities", entitesList);
        Map<String, Document> idsAndRecords = whelk.bulkLoad(recordIdsOnPage);
        for (Document doc : idsAndRecords.values()) {
            entitesList.add(doc.getThing());
        }

        String jsonResponse = mapper.writeValueAsString(responseObject);
        BufferedWriter writer = new BufferedWriter( res.getWriter() );
        writer.write(jsonResponse);
        writer.close();
    }

    private static void invalidateIfOld(Path dumpFilePath) {
        // TODO
    }

    private static void generateDump(Whelk whelk, Path dumpFilePath) {
        new Thread(() -> {
            try (BufferedWriter dumpFileWriter = new BufferedWriter(new FileWriter(dumpFilePath.toFile()));
                 Connection connection = whelk.getStorage().getOuterConnection()) {

                connection.setAutoCommit(false);
                String library = "https://libris.kb.se/library/Li"; // TEMP
                String sql = " SELECT " +
                        "  id" +
                        " FROM" +
                        "  lddb" +
                        " WHERE" +
                        "  data#>>'{@graph,1,heldBy,@id}' = ?";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);

                preparedStatement.setString(1, library);
                preparedStatement.setFetchSize(EmmChangeSet.TARGET_HITS_PER_PAGE);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {

                        // Each line must be exactly 17 bytes long, including the (unix-)line break.
                        String id = String.format("%-16s\n", resultSet.getString(1));
                        dumpFileWriter.write(id);
                    }
                }

                dumpFileWriter.write( String.format("%-17s", DUMP_END_MARKER) );
            } catch (IOException | SQLException e) {
                logger.error("Failed dump generation", e);
            }
        }).start();
    }
}

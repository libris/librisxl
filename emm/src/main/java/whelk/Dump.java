package whelk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

public class Dump {
    private static final Logger logger = LogManager.getLogger(Dump.class);
    private static final String DUMP_END_MARKER = "_DUMP_END_MARKER"; // Must be 16 bytes

    public static void sendDumpResponse(Whelk whelk, HttpServletRequest req, HttpServletResponse res) throws IOException, SQLException {
        String dump = req.getParameter("dump");
        String offset = req.getParameter("offset");

        String tmpDir = System.getProperty("java.io.tmpdir");
        Path dumpsPath = Paths.get(tmpDir, "dumps");
        Files.createDirectories(dumpsPath);
        Path dumpFilePath = dumpsPath.resolve(dump+".dump");

        generateDump(whelk, dumpFilePath);
        sendDumpPageResponse(dumpFilePath, offset, res);
    }

    private static void sendDumpPageResponse(Path dumpFilePath, String offset, HttpServletResponse res) {
        // Determine the state of this dump
        try {
            // Not begun writing yet ?
            while (!Files.exists(dumpFilePath)) {
                System.err.println("no dump yet..");
                Thread.sleep(10);
            }

            try (RandomAccessFile file = new RandomAccessFile(dumpFilePath.toFile(), "r")) {

                // Is the dump generation finished ?
                file.seek(file.length()-17);
                boolean dumpFinished = file.readLine().equals(DUMP_END_MARKER);

                // Not data enough for a full page yet ?
                while (!dumpFinished && file.length() < 17 * EmmChangeSet.TARGET_HITS_PER_PAGE) {
                    System.err.println("not enough lines..");
                    Thread.sleep(10);
                }

                //file.seek(offset);
                System.err.println("PAGE AVAILABLE!!\n");

            }
        } catch (IOException | InterruptedException e) {
            logger.error("Failed reading dumpfile: " + dumpFilePath, e);
        }
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

                dumpFileWriter.write( String.format("%-16s\n", DUMP_END_MARKER) );
            } catch (IOException | SQLException e) {
                logger.error("Failed dump generation", e);
            }
        }).start();
    }
}

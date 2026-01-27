package whelk.housekeeping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.diff.Diff;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static whelk.util.Jackson.mapper;

public class HistoryArchiver extends HouseKeeper {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Path prelimPath;
    private final Path walPath;
    private final String permanentLocationPattern;

    private final Whelk whelk;

    public HistoryArchiver(Whelk whelk) {
        this.whelk = whelk;

        String archiveRoot = whelk.getHistoryArchiveRoot();
        Path archiveRootPath = Paths.get(archiveRoot);
        try {
            Files.createDirectories(archiveRootPath);
        } catch (IOException ioe) {
            prelimPath = walPath = null;
            permanentLocationPattern = null;
            logger.error("Could not createDirectories: " + archiveRoot);
            return;
        }
        prelimPath = archiveRootPath.resolve("libris-history.json.lines.tmp");
        walPath = archiveRootPath.resolve("libris-history.json.lines.wal");
        permanentLocationPattern = archiveRootPath.resolve("libris-history-£.json.lines.gz").toString();
    }

    private String status = "OK";

    public String getName() {
        return "History Archiver";
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return "0 18 * * *";
    }

    private List<String> getIdBatchToArchive(Connection connection) throws SQLException {
        PreparedStatement statement;
        ResultSet resultSet;

        connection.setAutoCommit(false);
        //Timestamp cutoff = Timestamp.from(Instant.now().minus(60, ChronoUnit.DAYS));
        Timestamp cutoff = Timestamp.from(Instant.now());
        String sql = "SELECT id FROM lddb WHERE deleted = true AND modified < ? LIMIT 1000";
        statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, cutoff);
        resultSet = statement.executeQuery();

        List<String> result = new ArrayList<>(1000);
        while (resultSet.next()) {
            result.add( resultSet.getString("id") );
        }
        return result;
    }

    private void lockRows(List<String> ids, Connection connection) throws SQLException {
        // Locking these rows within a transaction prevents the theoretical race condition where
        // someone else tries to touch the records, and thereby their history as we're archiving it.

        String sql = "SELECT (id) FROM lddb WHERE id = ANY (?) FOR UPDATE";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setArray(1, connection.createArrayOf("TEXT", ids.toArray()));
        statement.executeQuery();
    }


    private String generateArchiveLogFor(List<String> ids) throws IOException, SQLException {
        // The archive log is gzipped json.lines, one line per record. So that it can be
        // easily appended to an existing archive log file, and 'grep':ed in for finding
        // individual records and their history.
        // Each record (line) is a json document with an original version of the record
        // followed by diffs for every change since.

        StringBuilder sb = new StringBuilder();
        for (String id : ids) {

            List<Document> versions = whelk.getStorage().loadAllVersions(id);

            List<List> diffs = new ArrayList<>();
            for (int i = 0; i < versions.size()-1; ++i) {
                diffs.add( Diff.diff(versions.get(i).data, versions.get(i+1).data) );
            }

            Map recordHistory = Map.of(
                    "original", versions.get(0).data,
                    "diffs", diffs
            );
            sb.append(mapper.writeValueAsString(recordHistory));
            sb.append("\n");
        }
        return sb.toString();
    }

    private void writeArchiveLog(String archiveLog) throws IOException {

        // Create the wal-file if it does not exist. Ignore the return, either true or false is fine.
        walPath.toFile().createNewFile();

        // Copy the existing WAL to the preliminary location (potentially overwriting a previous failed attempt)
        Files.copy(walPath, prelimPath, StandardCopyOption.REPLACE_EXISTING);

        // Append our new data to the end of the file at the preliminary location
        Files.write(prelimPath, archiveLog.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);

        // Move the preliminary file over the WAL file, replacing it.
        Files.move(prelimPath, walPath, StandardCopyOption.REPLACE_EXISTING);

        // Disk write durability is notoriously hard to guarantee, but perform an fsync() here and we've done about all we can.
        try (FileChannel channel = (FileChannel) Files.newByteChannel(walPath, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private void possiblyCheckpoint() throws IOException {
        // Checkpointing in this context means moving and gzipping the current WAL file into a date-named file for permanent storage.
        // Note, that a feature of the gzip-algorithm is that gzipped files *can* be appended to one another as is
        // without data loss and without any need for decompressing first. The resulting file will decompress to the same thing as
        // decompressing the two original files, one after the other and concatenating the results.
        // This means that if we later on want fewer/larger archive files, existing ones can be concatenated to one another using
        // normal cli-tools without fear of loss.

        // Is it time?
        boolean timeToCheckpoint = false;
        try (FileChannel walChannel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            long byteCount = walChannel.size();
            if (byteCount > 100L*1024L*1024L) // Larger than 100MB ?
                timeToCheckpoint = true;
        }

        if (timeToCheckpoint) {
            String permanentLocation = permanentLocationPattern.replaceAll("£", Instant.now().toString());
            Path permanentPath = Paths.get(permanentLocation);

            FileInputStream is = new FileInputStream(walPath.toFile());
            GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(permanentPath.toFile()));
            byte[] buf = new byte[4096];
            int byteCount = 0;
            while (byteCount != -1) { // EOF
                os.write(buf, 0, byteCount);
                byteCount = is.read(buf);
            }
            is.close();
            os.close();
            try (FileChannel channel = (FileChannel) Files.newByteChannel(permanentPath, StandardOpenOption.READ)) {
                channel.force(true);
            }

            Files.delete(walPath);
            logger.info("History archive checkpoint finalized. Moved the current WAL to: " + permanentLocation);
        }
    }

    public void trigger() {
        if (walPath == null) {
            logger.warn("No history archive root specified, not doing any history archiving.");
            return;
        }

        Connection connection = whelk.getStorage().getOuterConnection();

        try {

            // Work for up to ~15 minutes a day archiving history
            Instant start = Instant.now();
            while (Instant.now().isBefore( start.plus(15, ChronoUnit.MINUTES) )) {

                connection.setAutoCommit(false);
                List<String> ids = getIdBatchToArchive(connection);
                lockRows(ids, connection);
                String archiveLog = generateArchiveLogFor(ids);
                writeArchiveLog(archiveLog);

                // Delete history and record

                connection.commit();

                if (!ids.isEmpty()) {
                    logger.info("Archived history for " + ids.size() + " deleted records.");
                }

                possiblyCheckpoint();

                if (ids.isEmpty()) {
                    break; // Don't spin around doing nothing.
                }
            }

        } catch (Exception e) {
            status = e.getMessage();
            try {
                connection.rollback();
            } catch (SQLException sqle) {
                throw new RuntimeException(sqle);
            }
            throw new RuntimeException(e);
        }
        status = "OK";
    }
}

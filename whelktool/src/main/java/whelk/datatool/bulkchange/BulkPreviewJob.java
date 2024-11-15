package whelk.datatool.bulkchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.datatool.RecordedChange;
import whelk.datatool.WhelkTool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;

public class BulkPreviewJob extends BulkJob {
    private static final Logger log = LogManager.getLogger(BulkPreviewJob.class);

    public static final int RECORD_MAX_ITEMS = 500;

    WhelkTool tool;
    BulkJobDocument jobDoc;

    public BulkPreviewJob(Whelk whelk, String id) throws IOException {
        super(whelk, id);

        jobDoc = loadDocument();

        tool = buildWhelkTool(jobDoc.clone()); // guard against script modifying spec data
        tool.setDryRun(true);
        tool.setRecordChanges(true);
        tool.setRecordingLimit(RECORD_MAX_ITEMS);
        tool.setLogger(log);
    }

    public boolean isFinished() {
        return tool != null && tool.isFinished();
    }

    public void cancel() {
        // TODO?
        tool.setLimit(0);
    }

    public List<RecordedChange> getChanges() {
        if (tool.getErrorDetected() != null) {
            throw new RuntimeException(tool.getErrorDetected());
        }

        return tool.getRecordedChanges();
    }

    public boolean isSameVersion(BulkJobDocument jobDoc) {
        var a = this.jobDoc.getChecksum(whelk.getJsonld());
        var b = jobDoc.getChecksum(whelk.getJsonld());
        return a.equals(b);
    }

    @Override
    public void run() {
        try {
            var scriptLog = tool.getMainLog();

            scriptLog.println(String.format("Running %s: %s for %s", JOB_TYPE, id, jobDoc.getChangeAgentId()));
            scriptLog.println(String.format("label: %s", jobDoc.getLabels()));
            scriptLog.println(String.format("comment: %s", jobDoc.getComments()));
            scriptLog.println();

            printScriptLogHeader(tool, jobDoc);
            log.info("Running {}: {}", JOB_TYPE, id);
            tool.run();
        } catch (Exception e) {
            // TODO
            log.error(e);
            System.err.println(e);
        }
    }

    @Override
    protected File reportDir() throws IOException {
        return new File(Files.createTempDirectory(REPORTS_DIR).toFile(), executionId);
    }
}

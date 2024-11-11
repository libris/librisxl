package whelk.datatool.bulkchange;

import org.apache.log4j.Logger;
import whelk.Whelk;
import whelk.datatool.RecordedChange;
import whelk.datatool.WhelkTool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;

public class BulkPreviewJob extends BulkJob {
    public static final int RECORD_MAX_ITEMS = 500;

    private static final Logger logger = Logger.getLogger(BulkPreviewJob.class);

    WhelkTool tool;
    BulkJobDocument jobDoc;

    public BulkPreviewJob(Whelk whelk, String id) throws IOException {
        super(whelk, id);

        jobDoc = loadDocument();

        tool = buildWhelkTool(jobDoc);
        tool.setDryRun(true);
        tool.setRecordChanges(true);
        tool.setRecordingLimit(RECORD_MAX_ITEMS);
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
            logger.info(String.format("Running %s: %s", JOB_TYPE, id));

            tool.run();
        } catch (Exception e) {
            // TODO
            logger.error(e);
            System.err.println(e);
        }
    }

    @Override
    protected File reportDir() throws IOException {
        return new File(Files.createTempDirectory(REPORTS_DIR).toFile(), executionId);
    }
}

package whelk.datatool.bulkchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;
import whelk.datatool.bulkchange.BulkJobDocument.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static whelk.Document.HASH_IT;
import static whelk.datatool.WhelkTool.CREATED_LOG_NAME;
import static whelk.datatool.WhelkTool.DELETED_LOG_NAME;
import static whelk.datatool.WhelkTool.MAIN_LOG_NAME;
import static whelk.datatool.WhelkTool.MODIFIED_LOG_NAME;
import static whelk.datatool.WhelkTool.ValidationMode.LOG_ONLY;
import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Completed;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Failed;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Ready;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Running;
import static whelk.util.Unicode.stripPrefix;
import static whelk.util.Unicode.stripSuffix;

public class BulkJob implements Runnable {
    private static final Logger log = LogManager.getLogger(BulkJob.class);

    public static final String BULK_CONTEXT_PATH = "/_bulk-change";
    public static final String BULK_REPORTS_PATH = BULK_CONTEXT_PATH + "/reports";
    public static final Set<String> FORBIDDEN_REPORTS = Set.of(MAIN_LOG_NAME);

    protected static final String REPORTS_DIR = "bulk-change-reports";

    protected final String id;
    protected final String systemId;
    protected final Whelk whelk;
    protected final String executionId;

    public BulkJob(Whelk whelk, String id) {
        this.whelk = whelk;
        this.id = id;
        this.systemId = stripSuffix(stripPrefix(id, Document.getBASE_URI().toString()), Document.HASH_IT);
        this.executionId = executionId(systemId);
    }

    @Override
    public void run() {
        try {
            AtomicBoolean statusWasReady = new AtomicBoolean(false);
            storeUpdate(doc -> {
                if (doc.getStatus() == Ready) {
                    doc.setStatus(Running);
                    statusWasReady.set(true);
                }
            });

            if (!statusWasReady.get()) {
                return;
            }

            var jobDoc = loadDocument();
            var tool = buildWhelkTool(jobDoc);
            printScriptLogHeader(tool, jobDoc);
            log.info("Running {}: {}", JOB_TYPE, id);

            tool.run();

            if ((tool.getErrorDetected() != null)) {
                finish(Failed);
            } else {
                finish(Completed);
            }
        } catch (Exception e) {
            // TODO
            log.error(e);
            System.err.println(e);
            finish(Failed);
        }
    }

    private void finish(Status status) {
        storeUpdate(doc -> {
            doc.setStatus(status);
            doc.addExecution(
                    ZonedDateTime.now(ZoneId.systemDefault()),
                    status,
                    filteredReports(),
                    lineCount(CREATED_LOG_NAME),
                    lineCount(MODIFIED_LOG_NAME),
                    lineCount(DELETED_LOG_NAME));
        });
    }

    private long lineCount(String reportName) {
        try (Stream<String> stream = Files.lines(new File(reportDir(), reportName).toPath(), StandardCharsets.UTF_8)) {
            return stream.count();
        } catch(FileNotFoundException ignored) {
            return 0;
        } catch (IOException e) {
            log.warn("Could not get line count", e);
            return 0;
        }
    }

    private List<String> filteredReports() {
        try {
            var files = reportDir().listFiles(pathname ->
                pathname.isFile() && !FORBIDDEN_REPORTS.contains(pathname.getName())
            );

            if (files == null) {
                throw new IOException("Could not list files");
            }

            var path = BULK_REPORTS_PATH + "/" + reportDir().getName() + "/";
            return Arrays.stream(files)
                    .filter(f -> lineCount(f.getName()) > 0)
                    .map(f -> path + f.getName())
                    .toList();
        } catch (IOException e) {
            log.warn(e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    protected void printScriptLogHeader(WhelkTool tool, BulkJobDocument jobDoc) {
        var scriptLog = tool.getMainLog();

        scriptLog.println(String.format("Running %s: %s for %s", JOB_TYPE, id, jobDoc.getChangeAgentId()));
        scriptLog.println(String.format("label: %s", jobDoc.getLabels()));
        scriptLog.println(String.format("comment: %s", jobDoc.getComments()));
        scriptLog.println();
    }

    protected WhelkTool buildWhelkTool(BulkJobDocument jobDoc) throws IOException {

        // FIXME handle bulk job thing vs record id consistently
        var bulkJobThingId = stripSuffix(id, HASH_IT) + HASH_IT;

        Script script = jobDoc.getSpecification().getScript(bulkJobThingId);

        WhelkTool tool = new WhelkTool(whelk, script, reportDir(), WhelkTool.getDEFAULT_STATS_NUM_IDS());

        // TODO for now setting changedBy only works for loud changes (!minorChange in PostgreSQLComponent)
        tool.setDefaultChangedBy(jobDoc.getChangeAgentId());
        tool.setAllowLoud(jobDoc.shouldUpdateModifiedTimestamp());
        tool.setNoThreads(false);
        tool.setValidationMode(LOG_ONLY);

        return tool;
    }

    protected BulkJobDocument loadDocument() {
        return new BulkJobDocument(whelk.getDocument(systemId).data);
    }

    private void storeUpdate(Consumer<BulkJobDocument> updater) {
        var minorUpdate = true;
        var writeIdenticalVersions = false;
        var changedIn = "???";
        var changedBy = loadDocument().getDescriptionLastModifier();
        PostgreSQLComponent.UpdateAgent updateAgent = doc -> updater.accept(new BulkJobDocument(doc.data));
        whelk.storeAtomicUpdate(systemId, minorUpdate, writeIdenticalVersions, changedIn, changedBy, updateAgent);
    }

    protected File reportDir() throws IOException {
        return new File(reportBaseDir(whelk), executionId);
    }

    public static File reportBaseDir(Whelk whelk) throws IOException {
        return new File(whelk.getLogRoot(), REPORTS_DIR);
    }

    protected static String executionId(String baseName) {
        String now = LocalDateTime
                .now(ZoneId.systemDefault())
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .replace(":", "");

        String safeName = baseName.replaceAll("[^\\w.-]+", "");
        return String.format("%s-%s", safeName, now);
    }
}

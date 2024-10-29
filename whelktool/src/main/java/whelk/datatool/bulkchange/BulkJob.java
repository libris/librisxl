package whelk.datatool.bulkchange;

import org.apache.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Completed;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Failed;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Ready;
import static whelk.datatool.bulkchange.BulkJobDocument.Status.Running;
import static whelk.util.Unicode.stripPrefix;
import static whelk.util.Unicode.stripSuffix;

public class BulkJob implements Runnable {
    private static final Logger logger = Logger.getLogger(BulkJob.class);
    private static final String REPORTS_DIR = "bulk-change-reports";

    private final String id;
    private final String systemId;
    private final Whelk whelk;

    public BulkJob(Whelk whelk, String id) {
        this.whelk = whelk;
        this.id = id;
        this.systemId = stripSuffix(stripPrefix(id, Document.getBASE_URI().toString()), Document.HASH_IT);
    }

    @Override
    public void run() {
        try {
            AtomicBoolean shouldRun = new AtomicBoolean(false);
            storeUpdate(doc -> {
                BulkJobDocument jobDoc = new BulkJobDocument(doc.data);

                if (jobDoc.getStatus() == Ready) {
                    jobDoc.setStatus(Running);
                    shouldRun.set(true);
                }
            });

            if (shouldRun.get()) {
                var jobDoc = loadDocument();
                var tool = buildWhelkTool(jobDoc);
                var scriptLog = tool.getMainLog();

                scriptLog.println(String.format("Running %s: %s for %s", JOB_TYPE, id, jobDoc.getChangeAgentId()));
                scriptLog.println(String.format("label: %s", jobDoc.getLabels()));
                scriptLog.println(String.format("comment: %s", jobDoc.getComments()));
                scriptLog.println();

                logger.info(String.format("Running %s: %s", JOB_TYPE, id));

                tool.run();

                storeUpdate(doc -> new BulkJobDocument(doc.data).setStatus(Completed));
            }
        } catch (Exception e) {
            // TODO
            logger.error(e);
            System.err.println(e);
            storeUpdate(doc -> new BulkJobDocument(doc.data).setStatus(Failed));
        }
    }

    WhelkTool buildWhelkTool(BulkJobDocument jobDoc) {
        Script script = jobDoc.getSpecification().getScript(id);
        WhelkTool tool = new WhelkTool(whelk, script, reportDir(systemId), WhelkTool.getDEFAULT_STATS_NUM_IDS());
        // TODO for now setting changedBy only works for loud changes (!minorChange in PostgreSQLComponent)
        tool.setDefaultChangedBy(jobDoc.getChangeAgentId());
        tool.setAllowLoud(jobDoc.shouldUpdateModifiedTimestamp());
        tool.setNoThreads(false);

        return tool;
    }

    BulkJobDocument loadDocument() {
        return new BulkJobDocument(whelk.getDocument(systemId).data);
    }

    void storeUpdate(PostgreSQLComponent.UpdateAgent updateAgent) {
        var minorUpdate = true;
        var writeIdenticalVersions = false;
        var changedIn = "???";
        var changedBy = loadDocument().getDescriptionLastModifier();
        whelk.storeAtomicUpdate(systemId, minorUpdate, writeIdenticalVersions, changedIn, changedBy, updateAgent);
    }

    private File reportDir(String baseName) {
        String now = LocalDateTime
                .now(ZoneId.systemDefault())
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .replace(":", "");

        String safeName = baseName.replaceAll("[^\\w.-]+", "");
        String dir = String.format("%s-%s", safeName, now);
        return new File(new File(whelk.getLogRoot(), REPORTS_DIR), dir);
    }


}

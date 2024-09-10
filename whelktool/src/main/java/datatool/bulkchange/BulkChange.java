package datatool.bulkchange;

import datatool.Script;
import datatool.WhelkTool;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static whelk.util.Unicode.stripPrefix;

public class BulkChange implements Runnable {
    //private static final Logger logger = Logger.getLogger(BulkChange.class);
    private static final String REPORTS_DIR = "bulk-change-reports";

    public enum Status {
        DraftBulkChange,
        ReadyBulkChange,
        RunningBulkChange,
        CompletedBulkChange,
        FailedBulkChange,
    }

    public enum Type {
        BulkChange
    }

    public enum Prop {
        bulkChangeStatus,
        bulkChangeSpecification
    }

    private String id;
    private String systemId;
    private Whelk whelk;

    public BulkChange(Whelk whelk, String id) {
        this.whelk = whelk;
        this.id = id;
        this.systemId = stripPrefix(id, Document.getBASE_URI().toString());
    }

    @Override
    public void run() {
        try {
            AtomicBoolean shouldRun = new AtomicBoolean(false);
            storeUpdate(doc -> {
                BulkChangeDocument changeDoc = new BulkChangeDocument(doc.data);

                if (changeDoc.getStatus() == Status.ReadyBulkChange) {
                    changeDoc.setStatus(Status.RunningBulkChange);
                    shouldRun.set(true);
                }
            });

            if (shouldRun.get()) {
                var tool = buildWhelkTool();
                tool.run();
                storeUpdate(doc -> new BulkChangeDocument(doc.data).setStatus(Status.CompletedBulkChange));
            }
        } catch (Exception e) {
            // TODO
            //logger.error(e);
            System.err.println(e);
            storeUpdate(doc -> new BulkChangeDocument(doc.data).setStatus(Status.FailedBulkChange));
        }
    }

    WhelkTool buildWhelkTool() {
        Script script = new Script("", id);
        WhelkTool tool = new WhelkTool(whelk, script, reportDir(), WhelkTool.getDEFAULT_STATS_NUM_IDS());
        tool.setAllowLoud(false);
        return tool;
    }

    void storeUpdate(PostgreSQLComponent.UpdateAgent updateAgent) {
        var minorUpdate = true;
        var writeIdenticalVersions = false;
        var changedIn = "???";
        var changedBy = whelk.getDocument(systemId).getDescriptionLastModifier();
        whelk.storeAtomicUpdate(systemId, minorUpdate, writeIdenticalVersions, changedIn, changedBy, updateAgent);
    }

    private File reportDir() {
        String now = LocalDateTime
                .now(ZoneId.systemDefault())
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        String dir = String.format("%s-%s", now, systemId);
        return new File(new File (whelk.getLogRoot(), REPORTS_DIR), dir);
    }
}

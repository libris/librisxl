package whelk.datatool.bulkchange;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static whelk.util.Unicode.stripPrefix;
import static whelk.util.Unicode.stripSuffix;

public class BulkChange implements Runnable {
    private static final Logger logger = Logger.getLogger(BulkChange.class);
    private static final String REPORTS_DIR = "bulk-change-reports";

    public enum Status {
        DraftBulkChange,
        ReadyBulkChange,
        RunningBulkChange,
        CompletedBulkChange,
        FailedBulkChange,
    }

    public enum MetaChanges {
        LoudBulkChange,
        SilentBulkChange,
    }

    public enum Type {
        BulkChange,
        FormSpecification,
        DeleteSpecification
    }

    public enum Prop {
        bulkChangeStatus,
        bulkChangeMetaChanges,
        bulkChangeSpecification,
        comment,
        label,

        matchForm,
        targetForm,
    }

    private String id;
    private String systemId;
    private Whelk whelk;

    public BulkChange(Whelk whelk, String id) {
        this.whelk = whelk;
        this.id = id;
        this.systemId = stripSuffix(stripPrefix(id, Document.getBASE_URI().toString()), Document.HASH_IT);
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
                var changeDoc = loadDocument();
                var changeAgent = changeDoc.getDescriptionLastModifier();
                var tool = buildWhelkTool(changeDoc, changeAgent);
                var scriptLog = tool.getMainLog();

                scriptLog.println(String.format("Running %s: %s for %s", Type.BulkChange, id, changeAgent));
                scriptLog.println(String.format("label: %s", changeDoc.getLabels()));
                scriptLog.println(String.format("comment: %s", changeDoc.getComments()));
                scriptLog.println();

                logger.info(String.format("Running %s: %s", Type.BulkChange, id));

                tool.run();

                storeUpdate(doc -> new BulkChangeDocument(doc.data).setStatus(Status.CompletedBulkChange));
            }
        } catch (Exception e) {
            // TODO
            logger.error(e);
            System.err.println(e);
            storeUpdate(doc -> new BulkChangeDocument(doc.data).setStatus(Status.FailedBulkChange));
        }
    }

    WhelkTool buildWhelkTool(BulkChangeDocument changeDoc, String changeAgent) {
        record ScriptData(String fileName, Map<Object, Object> params) {}

        ScriptData scriptData = switch (changeDoc.getSpecification()) {
            case BulkChangeDocument.FormSpecification formSpecification ->
                    new ScriptData("transform.groovy",
                            Map.of(Prop.matchForm, formSpecification.matchForm(), Prop.targetForm, formSpecification.targetForm())
            );
            case BulkChangeDocument.DeleteSpecification deleteSpecification ->
                    new ScriptData("delete.groovy", Map.of(Prop.matchForm, deleteSpecification.matchForm())
            );
        };

        Script script = new Script(loadClasspathScriptSource(scriptData.fileName()), id);
        WhelkTool tool = new WhelkTool(whelk, script, reportDir(systemId), WhelkTool.getDEFAULT_STATS_NUM_IDS());
        // TODO for now setting changedBy only works for loud changes (!minorChange in PostgreSQLComponent)
        tool.setDefaultChangedBy(changeAgent);
        tool.setScriptParams(scriptData.params());
        tool.setAllowLoud(changeDoc.isLoud());
        tool.setNoThreads(false);

        return tool;
    }

    BulkChangeDocument loadDocument() {
        return new BulkChangeDocument(whelk.getDocument(systemId).data);
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
        return new File(new File (whelk.getLogRoot(), REPORTS_DIR), dir);
    }

    private String loadClasspathScriptSource(String scriptName) {
        String path = "bulk-change-scripts/" + scriptName;
        try (InputStream scriptStream = BulkChange.class.getClassLoader().getResourceAsStream(path)) {
            assert scriptStream != null;
            return IOUtils.toString(new InputStreamReader(scriptStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

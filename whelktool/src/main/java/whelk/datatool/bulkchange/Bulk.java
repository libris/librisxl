package whelk.datatool.bulkchange;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;
import whelk.datatool.bulkchange.BulkJobDocument.Create;
import whelk.datatool.bulkchange.BulkJobDocument.Delete;
import whelk.datatool.bulkchange.BulkJobDocument.Update;
import whelk.util.JsonLdKey;

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

import static whelk.datatool.bulkchange.Bulk.Other.matchForm;
import static whelk.datatool.bulkchange.Bulk.Other.targetForm;
import static whelk.datatool.bulkchange.Bulk.Status.Ready;
import static whelk.datatool.bulkchange.Bulk.Status.Running;
import static whelk.datatool.bulkchange.Bulk.Other.Job;
import static whelk.util.Unicode.stripPrefix;
import static whelk.util.Unicode.stripSuffix;

public class Bulk implements Runnable {
    private static final Logger logger = Logger.getLogger(Bulk.class);
    private static final String REPORTS_DIR = "bulk-change-reports";

    public enum Status implements JsonLdKey {
        Draft("bulk:Draft"),
        Ready("bulk:Ready"),
        Running("bulk:Running"),
        Completed("bulk:Completed"),
        Failed("bulk:Failed");

        private final String key;

        Status(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }

    public enum Spec implements JsonLdKey {
        Update("bulk:Update"),
        Delete("bulk:Delete"),
        Create("bulk:Create");

        private final String key;

        Spec(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }

    public enum Other implements JsonLdKey {
        Job("bulk:Job"),

        status("bulk:status"),
        changeSpec("bulk:changeSpec"),
        shouldUpdateModifiedTimestamp("bulk:shouldUpdateModifiedTimestamp"),
        comment("comment"),
        label("label"),
        matchForm("bulk:matchForm"),
        targetForm("bulk:targetForm");

        private final String key;

        Other(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }

    private final String id;
    private final String systemId;
    private final Whelk whelk;

    public Bulk(Whelk whelk, String id) {
        this.whelk = whelk;
        this.id = id;
        this.systemId = stripSuffix(stripPrefix(id, Document.getBASE_URI().toString()), Document.HASH_IT);
    }

    @Override
    public void run() {
        try {
            AtomicBoolean shouldRun = new AtomicBoolean(false);
            storeUpdate(doc -> {
                BulkJobDocument changeDoc = new BulkJobDocument(doc.data);

                if (changeDoc.getStatus() == Ready) {
                    changeDoc.setStatus(Running);
                    shouldRun.set(true);
                }
            });

            if (shouldRun.get()) {
                var changeDoc = loadDocument();
                var changeAgent = changeDoc.getDescriptionLastModifier();
                var tool = buildWhelkTool(changeDoc, changeAgent);
                var scriptLog = tool.getMainLog();

                scriptLog.println(String.format("Running %s: %s for %s", Job, id, changeAgent));
                scriptLog.println(String.format("label: %s", changeDoc.getLabels()));
                scriptLog.println(String.format("comment: %s", changeDoc.getComments()));
                scriptLog.println();

                logger.info(String.format("Running %s: %s", Job, id));

                tool.run();

                storeUpdate(doc -> new BulkJobDocument(doc.data).setStatus(Status.Completed));
            }
        } catch (Exception e) {
            // TODO
            logger.error(e);
            System.err.println(e);
            storeUpdate(doc -> new BulkJobDocument(doc.data).setStatus(Status.Failed));
        }
    }

    WhelkTool buildWhelkTool(BulkJobDocument changeDoc, String changeAgent) {
        Script script = switch (changeDoc.getSpecification()) {
            case Update spec -> {
                Script s = new Script(loadClasspathScriptSource("update.groovy"), id);
                s.setParameters(Map.of(
                        matchForm, spec.matchForm(),
                        targetForm, spec.targetForm()
                ));
                yield s;
            }
            case Create spec -> {
                Script s = new Script(loadClasspathScriptSource("create.groovy"), id);
                s.setParameters(Map.of(
                        targetForm, spec.targetForm()
                ));
                yield s;
            }
            case Delete spec -> {
                Script s = new Script(loadClasspathScriptSource("delete.groovy"), id);
                s.setParameters(Map.of(
                        matchForm, spec.matchForm()
                ));
                yield s;
            }
        };

        WhelkTool tool = new WhelkTool(whelk, script, reportDir(systemId), WhelkTool.getDEFAULT_STATS_NUM_IDS());
        // TODO for now setting changedBy only works for loud changes (!minorChange in PostgreSQLComponent)
        tool.setDefaultChangedBy(changeAgent);
        tool.setAllowLoud(changeDoc.shouldUpdateModifiedTimestamp());
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

    private String loadClasspathScriptSource(String scriptName) {
        String path = "bulk-change-scripts/" + scriptName;
        try (InputStream scriptStream = Bulk.class.getClassLoader().getResourceAsStream(path)) {
            assert scriptStream != null;
            return IOUtils.toString(new InputStreamReader(scriptStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

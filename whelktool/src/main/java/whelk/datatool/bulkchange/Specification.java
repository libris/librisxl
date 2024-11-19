package whelk.datatool.bulkchange;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import whelk.Document;
import whelk.Whelk;
import whelk.datatool.Script;
import whelk.datatool.form.ModifiedThing;
import whelk.datatool.form.Transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.GRAPH_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.ADD_SUBJECT_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.REMOVE_SUBDIVISION_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY;

public sealed interface Specification permits Specification.Create, Specification.Delete, Specification.Merge, Specification.Update, Specification.Other {

    Script getScript(String bulkJobId);

    record Update(Map<String, Object> matchForm, Map<String, Object> targetForm) implements Specification {
        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("update.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    MATCH_FORM_KEY, matchForm,
                    TARGET_FORM_KEY, targetForm
            ));
            return s;
        }

        public Transform getTransform(Whelk whelk) {
            return new Transform(matchForm, targetForm, whelk);
        }
    }

    record Delete(Map<String, Object> matchForm) implements Specification {
        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("delete.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    MATCH_FORM_KEY, matchForm
            ));
            return s;
        }
    }

    record Create(Map<String, Object> targetForm) implements Specification {
        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("create.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    TARGET_FORM_KEY, targetForm
            ));
            return s;
        }
    }

    record Merge(Map<String, String> deprecate, Map<String, String> keep) implements Specification {
        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("merge.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    DEPRECATE_KEY, deprecate,
                    KEEP_KEY, keep
            ));
            return s;
        }
    }

    record Other(String name, Map<String, ?> parameters) implements Specification {
        private static final Map<String, List<String>> ALLOWED_SCRIPTS_PARAMS = Map.of(
                "removeSubdivision", List.of(REMOVE_SUBDIVISION_KEY, ADD_SUBJECT_KEY)
        );

        @Override
        public Script getScript(String bulkJobId) {
            if (!ALLOWED_SCRIPTS_PARAMS.containsKey(name)) {
                throw new IllegalArgumentException("Script " + name + " not supported");
            }

            Script s = new Script(loadClasspathScriptSource(name +".groovy"), bulkJobId);

            Map<Object, Object> params = new HashMap<>();
            params.putAll(Maps.filterKeys(parameters, k -> ALLOWED_SCRIPTS_PARAMS.get(name).contains(k)));
            s.setParameters(params);
            return s;
        }
    }

    private static String loadClasspathScriptSource(String scriptName) {
        String path = "bulk-change-scripts/" + scriptName;
        try (InputStream scriptStream = Specification.class.getClassLoader().getResourceAsStream(path)) {
            assert scriptStream != null;
            return IOUtils.toString(new InputStreamReader(scriptStream));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

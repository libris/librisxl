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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.GRAPH_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.ADD_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY;

public sealed interface Specification permits Specification.Create, Specification.Delete, Specification.Merge, Specification.Update, Specification.Other {

    Script getScript(String bulkJobId);

    final class Update implements Specification {
        private final Map<String, Object> matchForm;
        private final Map<String, Object> targetForm;

        private Transform transform;

        public Update(Map<String, Object> matchForm, Map<String, Object> targetForm) {
            this.matchForm = matchForm;
            this.targetForm = targetForm;
        }

        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("update.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    MATCH_FORM_KEY, matchForm,
                    TARGET_FORM_KEY, targetForm
            ));
            return s;
        }

        @SuppressWarnings("unchecked")
        public boolean modify(Document doc, Whelk whelk) {
            Map<String, Object> thing = doc.getThing();
            thing.put(RECORD_KEY, doc.getRecord());

            var m = new ModifiedThing(thing, getTransform(whelk), whelk.getJsonld().repeatableTerms);

            ((List<Map<?,?>>) doc.data.get(GRAPH_KEY)).set(0, (Map<?, ?>) m.getAfter().remove(RECORD_KEY));
            ((List<Map<?,?>>) doc.data.get(GRAPH_KEY)).set(1, m.getAfter());

            return m.isModified();
        }

        public Transform getTransform(Whelk whelk) {
            if (transform == null) {
                transform = new Transform(matchForm, targetForm, whelk);
            }
            return transform;
        }
    }

    final class Delete implements Specification {
        private final Map<String, Object> matchForm;

        private Transform.MatchForm matchFormObj;

        public Delete(Map<String, Object> matchForm) {
            this.matchForm = matchForm;
        }

        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("delete.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    MATCH_FORM_KEY, matchForm
            ));
            return s;
        }

        @SuppressWarnings("unchecked")
        public boolean matches(Document doc, Whelk whelk) {
            Map<String, Object> thing = doc.clone().getThing();
            thing.put(RECORD_KEY, doc.getRecord());
            return getMatchForm(whelk).matches(thing);
        }

        public Transform.MatchForm getMatchForm(Whelk whelk) {
            if (matchFormObj == null) {
                matchFormObj = new Transform.MatchForm(matchForm, whelk);
            }
            return matchFormObj;
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
                "removeTopicSubdivision", List.of(DEPRECATE_KEY, ADD_KEY)
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

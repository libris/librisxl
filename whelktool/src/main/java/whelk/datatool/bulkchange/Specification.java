package whelk.datatool.bulkchange;

import org.apache.commons.io.IOUtils;
import whelk.Whelk;
import whelk.datatool.Script;
import whelk.datatool.form.Transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY;

public sealed interface Specification permits Specification.Create, Specification.Delete, Specification.Update {

    Script getScript(String bulkJobId);
    Transform getTransform(Whelk whelk);

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

        @Override
        public Transform getTransform(Whelk whelk) {
            return new Transform(matchForm, targetForm, whelk);
        }
    }

    record Delete(Map<String, Object> matchForm) implements Specification {
        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("delete.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    TARGET_FORM_KEY, matchForm
            ));
            return s;
        }

        @Override
        public Transform.MatchForm getTransform(Whelk whelk) {
            return new Transform.MatchForm(matchForm, whelk);
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

        @Override
        public Transform getTransform(Whelk whelk) {
            // TODO
            return null;
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

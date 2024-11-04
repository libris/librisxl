package whelk.datatool.bulkchange;

import org.apache.commons.io.IOUtils;
import whelk.Document;
import whelk.Whelk;
import whelk.datatool.Script;
import whelk.datatool.form.ModifiedThing;
import whelk.datatool.form.Transform;
import whelk.util.DocumentUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.MATCH_FORM_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY;
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY;

public sealed interface Specification permits Specification.Create, Specification.Delete, Specification.Merge, Specification.Update {

    Script getScript(String bulkJobId);

    List<String> findAffectedIds(Whelk whelk);

    default Map<?, ?> getAfter(Map<String, Object> thing, Whelk whelk) {
        return Collections.emptyMap();
    }

    final class Update implements Specification {
        private final Map<String, Object> matchForm;
        private final Map<String, Object> targetForm;

        private Transform transform;

        Update(Map<String, Object> matchForm, Map<String, Object> targetForm) {
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

        @Override
        public List<String> findAffectedIds(Whelk whelk) {
            return queryIdsByForm(getTransform(whelk), whelk);
        }

        @Override
        public Map<?, ?> getAfter(Map<String, Object> thing, Whelk whelk) {
            return new ModifiedThing(thing, getTransform(whelk), whelk.getJsonld().repeatableTerms).getAfter();
        }

        public Transform getTransform(Whelk whelk) {
            if (transform == null) {
                transform = new Transform(matchForm, targetForm, whelk);
            }
            return transform;
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

        @Override
        public List<String> findAffectedIds(Whelk whelk) {
            return queryIdsByForm(new Transform.MatchForm(matchForm, whelk), whelk);
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
        public List<String> findAffectedIds(Whelk whelk) {
            return Collections.emptyList();
        }
    }

    final class Merge implements Specification {
        Collection<String> deprecate;
        String keep;

        private Set<String> obsoleteThingIdentifiers;
        private Map<String, String> thingUriToShortId;

        public Merge(Collection<String> deprecate, String keep) {
            this.deprecate = deprecate;
            this.keep = keep;
        }

        @Override
        public Script getScript(String bulkJobId) {
            Script s = new Script(loadClasspathScriptSource("merge.groovy"), bulkJobId);
            s.setParameters(Map.of(
                    DEPRECATE_KEY, deprecate,
                    KEEP_KEY, keep
            ));
            return s;
        }

        @Override
        public List<String> findAffectedIds(Whelk whelk) {
            // Include deprecated? Have to be shown as removed in preview.
            return Stream.concat(Stream.of(keep), getDependers(whelk).stream()).toList();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<?, ?> getAfter(Map<String, Object> thing, Whelk whelk) {
            var copy = (Map<String, Object>) Document.deepCopy(thing);
            if (keep.equals(thing.get(ID_KEY))) {
                addSameAsLinks(copy, whelk);
            } else {
                relink(copy, whelk);
            }
            return copy;
        }

        @SuppressWarnings("unchecked")
        public void addSameAsLinks(Map<String, Object> thing, Whelk whelk) {
            var origSameAs = (List<String>) thing.getOrDefault("sameAs", Collections.emptyList());
            var newSameAs = Stream.concat(origSameAs.stream(), getObsoleteThingIdentifiers(whelk).stream().map(uri -> Map.of(ID_KEY, uri)))
                    .distinct()
                    .toList();
            thing.put("sameAs", newSameAs);
        }

        public List<String> getDependers(Whelk whelk) {
            return deprecate.stream()
                    .map(uri -> getShortId(uri, whelk))
                    .map(whelk.getStorage()::getDependers)
                    .flatMap(SortedSet::stream)
                    .toList();
        }

        public boolean relink(Object data, Whelk whelk) {
            return DocumentUtil.traverse(data, (value, path) -> {
                if (!path.isEmpty() && ID_KEY.equals(path.getLast()) && getObsoleteThingIdentifiers(whelk).contains(value)) {
                    // What if there are links to a record uri?
                    return new DocumentUtil.Replace(keep);
                }
                return new DocumentUtil.Nop();
            });
        }

        private String getShortId(String thingUri, Whelk whelk) {
            if (thingUriToShortId == null) {
                this.thingUriToShortId = new HashMap<>();
            }
            if (thingUriToShortId.get(thingUri) == null) {
                String shortId = whelk.getStorage().getDocumentByIri(thingUri).getShortId();
                this.thingUriToShortId.put(thingUri, shortId);
            }
            return thingUriToShortId.get(thingUri);
        }

        private Set<String> getObsoleteThingIdentifiers(Whelk whelk) {
            if (obsoleteThingIdentifiers == null) {
                this.obsoleteThingIdentifiers = whelk.bulkLoad(deprecate)
                        .values()
                        .stream()
                        .flatMap(doc -> doc.getThingIdentifiers().stream())
                        .collect(Collectors.toSet());
            }
            return obsoleteThingIdentifiers;
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

    private static List<String> queryIdsByForm(Transform transform, Whelk whelk) {
        // TODO use COUNT + LIMIT & OFFSET and don't fetch all ids every time
        var sparqlPattern = transform.getSparqlPattern(whelk.getJsonld().context);
        return whelk.getSparqlQueryClient()
                .queryIdsByPattern(sparqlPattern)
                .stream()
                .sorted()
                .toList();
    }
}

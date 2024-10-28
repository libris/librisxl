package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;
import whelk.datatool.bulkchange.Bulk.Spec;
import whelk.datatool.bulkchange.Bulk.Status;
import whelk.exception.ModelValidationException;
import whelk.util.DocumentUtil;
import whelk.util.JsonLdKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.datatool.bulkchange.Bulk.Other.changeSpec;
import static whelk.datatool.bulkchange.Bulk.Other.comment;
import static whelk.datatool.bulkchange.Bulk.Other.label;
import static whelk.datatool.bulkchange.Bulk.Other.matchForm;
import static whelk.datatool.bulkchange.Bulk.Other.shouldUpdateModifiedTimestamp;
import static whelk.datatool.bulkchange.Bulk.Other.status;
import static whelk.datatool.bulkchange.Bulk.Other.targetForm;
import static whelk.util.JsonLdKey.fromKey;

public class BulkJobDocument extends Document {

    public sealed interface Specification permits Update, Create, Delete {
    }

    public record Update(Map<String, Object> matchForm, Map<String, Object> targetForm) implements Specification { }
    public record Create(Map<String, Object> targetForm) implements Specification { }
    public record Delete(Map<String, Object> matchForm) implements Specification { }

    private static final List<Object> STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, status.key()); // FIXME used in _set so can't use enum directly
    private static final List<Object> UPDATE_TIMESTAMP_PATH = List.of(JsonLd.GRAPH_KEY, 1, shouldUpdateModifiedTimestamp);
    private static final List<Object> LABELS_PATH = List.of(JsonLd.GRAPH_KEY, 1, label, "*");
    private static final List<Object> COMMENTS_PATH = List.of(JsonLd.GRAPH_KEY, 1, comment, "*");
    private static final List<Object> SPECIFICATION_PATH = List.of(JsonLd.GRAPH_KEY, 1, changeSpec);

    public BulkJobDocument(Document doc) {
        this(doc.data);
    }

    public BulkJobDocument(Map<?, ?> data) {
        super(data);

        if (!Bulk.Other.Job.toString().equals(getThingType())) {
            throw new ModelValidationException("Document is not a " + Bulk.Other.Job);
        }
    }

    public Status getStatus() {
        return fromKey(Status.class, get(data, STATUS_PATH));
    }

    public void setStatus(Status status) {
        _set(STATUS_PATH, status.key(), data);
    }

    public List<String> getLabels() {
        return get(LABELS_PATH, Collections.emptyList());
    }

    public List<String> getComments() {
        return get(COMMENTS_PATH, Collections.emptyList());
    }

    public boolean shouldUpdateModifiedTimestamp() {
        return get(data, UPDATE_TIMESTAMP_PATH, false);
    }

    public Map<String, Object> getSpecificationRaw() {
        return get(data, SPECIFICATION_PATH);
    }

    public Specification getSpecification() {
        Map<String, Object> spec = getSpecificationRaw();
        if (spec == null) {
            throw new ModelValidationException("Nothing in " + SPECIFICATION_PATH);
        }

        String type = get(spec, JsonLd.TYPE_KEY);
        return switch(fromKey(Spec.class, type)) {
            case Spec.Update -> new Update(
                    get(spec, matchForm, Collections.emptyMap()),
                    get(spec, targetForm, Collections.emptyMap())
            );
            case Spec.Delete -> new Delete(
                    get(spec, matchForm, Collections.emptyMap())
            );
            case Spec.Create -> new Create(
                    get(spec, targetForm, Collections.emptyMap())
            );
            case null -> throw new ModelValidationException(String.format("Bad %s: %s", changeSpec, spec));
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Object thing, List<Object> path, T defaultTo) {
        return (T) DocumentUtil.getAtPath(thing, path, defaultTo);
    }

    private <T> T get(Object thing, List<Object> path) {
        return get(thing, path, null);
    }

    private <T> T get(Object thing, JsonLdKey key, T defaultTo) {
        return get(thing, List.of(key), defaultTo);
    }

    private <T> T get(Object thing, JsonLdKey key) {
        return get(thing, List.of(key), null);
    }

    private <T> T get(Object thing, String key) {
        return get(thing, List.of(key), null);
    }
}

package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;
import whelk.exception.ModelValidationException;
import whelk.util.DocumentUtil;
import whelk.util.JsonLdKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.util.JsonLdKey.fromKey;

// All terms are defined in https://github.com/libris/definitions/blob/develop/source/vocab/platform.ttl
public class BulkJobDocument extends Document {

    public enum SpecType implements JsonLdKey {
        Update("bulk:Update"),
        Delete("bulk:Delete"),
        Create("bulk:Create");

        private final String key;

        SpecType(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }

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

    public static final String JOB_TYPE = "bulk:Job";
    public static final String STATUS_KEY = "bulk:status";
    public static final String CHANGE_SPEC_KEY = "bulk:changeSpec";
    public static final String SHOULD_UPDATE_TIMESTAMP_KEY = "bulk:shouldUpdateModifiedTimestamp";
    public static final String MATCH_FORM_KEY = "bulk:matchForm";
    public static final String TARGET_FORM_KEY = "bulk:targetForm";
    public static final String COMMENT_KEY = "comment";
    public static final String LABEL_KEY = "label";

    private static final List<Object> STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, STATUS_KEY);
    private static final List<Object> UPDATE_TIMESTAMP_PATH = List.of(JsonLd.GRAPH_KEY, 1, SHOULD_UPDATE_TIMESTAMP_KEY);
    private static final List<Object> LABELS_PATH = List.of(JsonLd.GRAPH_KEY, 1, LABEL_KEY, "*");
    private static final List<Object> COMMENTS_PATH = List.of(JsonLd.GRAPH_KEY, 1, COMMENT_KEY, "*");
    private static final List<Object> SPECIFICATION_PATH = List.of(JsonLd.GRAPH_KEY, 1, CHANGE_SPEC_KEY);

    public BulkJobDocument(Document doc) {
        this(doc.data);
    }

    public BulkJobDocument(Map<?, ?> data) {
        super(data);

        if (!JOB_TYPE.equals(getThingType())) {
            throw new ModelValidationException("Document is not a " + JOB_TYPE);
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

        String specType = get(spec, JsonLd.TYPE_KEY);
        return switch(fromKey(SpecType.class, specType)) {
            case SpecType.Update -> new Specification.Update(
                    get(spec, TARGET_FORM_KEY, Collections.emptyMap()),
                    get(spec, TARGET_FORM_KEY, Collections.emptyMap())
            );
            case SpecType.Delete -> new Specification.Delete(
                    get(spec, MATCH_FORM_KEY, Collections.emptyMap())
            );
            case SpecType.Create -> new Specification.Create(
                    get(spec, TARGET_FORM_KEY, Collections.emptyMap())
            );
            case null -> throw new ModelValidationException(String.format("Bad %s %s: %s",
                    CHANGE_SPEC_KEY, JsonLd.TYPE_KEY, specType));
        };
    }

    public String getChangeAgentId() {
        return getDescriptionLastModifier();
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Object thing, List<Object> path, T defaultTo) {
        return (T) DocumentUtil.getAtPath(thing, path, defaultTo);
    }

    private <T> T get(Object thing, List<Object> path) {
        return get(thing, path, null);
    }

    private <T> T get(Object thing, String key) {
        return get(thing, List.of(key), null);
    }

    private <T> T get(Object thing, String key, T defaultTo) {
        return get(thing, List.of(key), defaultTo);
    }
}

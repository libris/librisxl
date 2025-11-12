package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;
import whelk.exception.ModelValidationException;
import whelk.util.DocumentUtil;
import whelk.util.JsonLdKey;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.asList;
import static whelk.util.JsonLdKey.fromKey;

// All terms are defined in https://github.com/libris/definitions/blob/develop/source/vocab/platform.ttl
public class BulkJobDocument extends Document {

    public enum SpecType implements JsonLdKey {
        Update("bulk:Update"),
        Delete("bulk:Delete"),
        Create("bulk:Create"),
        Merge("bulk:Merge"),
        Other("bulk:Other");

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
    public static final String KEEP_KEY = "bulk:keep";
    public static final String DEPRECATE_KEY = "bulk:deprecate";
    public static final String REMOVE_SUBDIVISION_KEY = "bulk:removeSubdivision";
    public static final String ADD_TERM_KEY = "bulk:addTerm";
    public static final String SCRIPT_KEY = "bulk:script";
    public static final String EXECUTION_KEY = "bulk:execution";
    public static final String EXECUTION_TYPE = "bulk:Execution";
    public static final String REPORT_KEY = "bulk:report";
    public static final String END_TIME_KEY = "endTime";
    public static final String NUM_CREATED_KEY = "bulk:numCreated";
    public static final String NUM_UPDATED_KEY = "bulk:numUpdated";
    public static final String NUM_DELETED_KEY = "bulk:numDeleted";
    public static final String NUM_FAILED_KEY = "bulk:numFailed";

    private static final List<Object> STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, STATUS_KEY);
    private static final List<Object> UPDATE_TIMESTAMP_PATH = List.of(JsonLd.GRAPH_KEY, 1, SHOULD_UPDATE_TIMESTAMP_KEY);
    private static final List<Object> LABELS_PATH = List.of(JsonLd.GRAPH_KEY, 1, LABEL_KEY, "*");
    private static final List<Object> COMMENTS_PATH = List.of(JsonLd.GRAPH_KEY, 1, COMMENT_KEY, "*");
    private static final List<Object> SPECIFICATION_PATH = List.of(JsonLd.GRAPH_KEY, 1, CHANGE_SPEC_KEY);
    private static final List<Object> EXECUTION_PATH = List.of(JsonLd.GRAPH_KEY, 1, EXECUTION_KEY);

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

    @SuppressWarnings("unchecked")
    public void addExecution(ZonedDateTime endTime, Status status, List<String> reportPaths,
                             long numCreated, long numUpdated, long numDeleted, long numFailed) {
        var e = new HashMap<>(Map.of(
                JsonLd.TYPE_KEY, EXECUTION_TYPE,
                REPORT_KEY, reportPaths.stream().map(s -> Map.of(ID_KEY, s)).toList(),
                END_TIME_KEY, endTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                STATUS_KEY, status.key()
        ));

        if (numCreated > 0) {
            e.put(NUM_CREATED_KEY, numCreated);
        }
        if (numUpdated > 0) {
            e.put(NUM_UPDATED_KEY, numUpdated);
        }
        if (numDeleted > 0) {
            e.put(NUM_DELETED_KEY, numDeleted);
        }
        if (numFailed > 0) {
            e.put(NUM_FAILED_KEY, numFailed);
        }

        var executions = asList(get(data, EXECUTION_PATH));
        executions.add(e);
        _set(EXECUTION_PATH, executions, data);
    }

    public SpecType getSpecificationType() {
        Map<String, Object> spec = getSpecificationRaw();
        if (spec == null) {
            throw new ModelValidationException("Nothing in " + SPECIFICATION_PATH);
        }
        return fromKey(SpecType.class, get(spec, JsonLd.TYPE_KEY));
    }

    public Specification getSpecification() {
        return switch(getSpecificationType()) {
            case SpecType.Update -> new Specification.Update(
                    get(spec, MATCH_FORM_KEY, Collections.emptyMap()),
                    get(spec, TARGET_FORM_KEY, Collections.emptyMap())
            );
            case SpecType.Delete -> new Specification.Delete(
                    get(spec, MATCH_FORM_KEY, Collections.emptyMap())
            );
            case SpecType.Create -> new Specification.Create(
                    get(spec, TARGET_FORM_KEY, Collections.emptyMap())
            );
            case SpecType.Merge -> new Specification.Merge(
                    get(spec, DEPRECATE_KEY, Collections.emptyMap()),
                    get(spec, KEEP_KEY, Collections.emptyMap())
            );
            case SpecType.Other -> new Specification.Other(
                    get(spec, SCRIPT_KEY, null),
                    spec
            );
            case null -> throw new ModelValidationException(String.format("Bad %s %s: %s",
                    CHANGE_SPEC_KEY, JsonLd.TYPE_KEY, specType));
        };
    }

    public String getChangeAgentId() {
        return getDescriptionLastModifier();
    }

    public BulkJobDocument clone() {
        return new BulkJobDocument(super.clone());
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

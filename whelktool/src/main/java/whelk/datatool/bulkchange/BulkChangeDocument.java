package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;
import whelk.exception.ModelValidationException;
import whelk.util.DocumentUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.datatool.bulkchange.BulkChange.Prop.bulkChangeMetaChanges;
import static whelk.datatool.bulkchange.BulkChange.Prop.bulkChangeSpecification;
import static whelk.datatool.bulkchange.BulkChange.Prop.bulkChangeStatus;
import static whelk.datatool.bulkchange.BulkChange.Prop.comment;
import static whelk.datatool.bulkchange.BulkChange.Prop.label;
import static whelk.datatool.bulkchange.BulkChange.Type.FormSpecification;
import static whelk.datatool.bulkchange.BulkChange.Type.DeleteSpecification;

public class BulkChangeDocument extends Document {

    public sealed interface Specification permits FormSpecification, DeleteSpecification {
    }

    public record FormSpecification(Map<String, Object> matchForm, Map<String, Object> targetForm) implements Specification { }
    public record DeleteSpecification(Map<String, Object> matchForm) implements Specification { }

    private static final List<Object> STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, bulkChangeStatus.toString()); // FIXME used in _set so can't use enum directly
    private static final List<Object> META_PATH = List.of(JsonLd.GRAPH_KEY, 1, bulkChangeMetaChanges);
    private static final List<Object> LABELS_PATH = List.of(JsonLd.GRAPH_KEY, 1, label, "*");
    private static final List<Object> COMMENTS_PATH = List.of(JsonLd.GRAPH_KEY, 1, comment, "*");
    private static final List<Object> SPECIFICATION_PATH = List.of(JsonLd.GRAPH_KEY, 1, bulkChangeSpecification);

    public BulkChangeDocument(Document doc) {
        this(doc.data);
    }

    public BulkChangeDocument(Map<?, ?> data) {
        super(data);

        if (!BulkChange.Type.BulkChange.toString().equals(getThingType())) {
            throw new ModelValidationException("Document is not a " + BulkChange.Type.BulkChange);
        }
    }

    public BulkChange.Status getStatus() {
        return BulkChange.Status.valueOf(get(STATUS_PATH, null));
    }

    public void setStatus(BulkChange.Status status) {
        _set(STATUS_PATH, status.toString(), data);
    }

    public BulkChange.MetaChanges getMetaChanges() {
        return BulkChange.MetaChanges.valueOf(get(META_PATH, BulkChange.MetaChanges.SilentBulkChange.toString()));
    }

    public List<String> getLabels() {
        return get(LABELS_PATH, Collections.emptyList());
    }

    public List<String> getComments() {
        return get(COMMENTS_PATH, Collections.emptyList());
    }

    public boolean isLoud() {
        return getMetaChanges() == BulkChange.MetaChanges.LoudBulkChange;
    }

    public Map<String, Object> getSpecificationRaw() {
        return get(SPECIFICATION_PATH, null);
    }

    public Specification getSpecification() {
        Map<String, Object> spec = getSpecificationRaw();
        if (spec == null) {
            throw new ModelValidationException("Nothing in " + SPECIFICATION_PATH);
        }

        var specType = spec.get(JsonLd.TYPE_KEY);

        if (FormSpecification.toString().equals(specType)) {
            return new FormSpecification(
                    get(spec, List.of(BulkChange.Prop.matchForm.toString()), Collections.emptyMap()),
                    get(spec, List.of(BulkChange.Prop.targetForm.toString()), Collections.emptyMap())
            );
        } else if (DeleteSpecification.toString().equals(specType)) {
            return new DeleteSpecification(
                    get(spec, List.of(BulkChange.Prop.matchForm.toString()), Collections.emptyMap())
            );
        }

        throw new ModelValidationException(String.format("Bad %s: %s", bulkChangeSpecification, spec));
    }

    private <T> T get(List<Object> path, T defaultTo) {
        return get(data, path, defaultTo);
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Object thing, List<Object> path, T defaultTo) {
        return (T) DocumentUtil.getAtPath(thing, path, defaultTo);
    }
}

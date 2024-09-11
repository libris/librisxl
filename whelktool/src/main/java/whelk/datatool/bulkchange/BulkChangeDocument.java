package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;
import whelk.search2.parse.Ast;
import whelk.util.DocumentUtil;

import javax.print.Doc;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BulkChangeDocument extends Document {

    public sealed interface Specification permits FormSpecification {
    }

    public record FormSpecification(Map<String, Object> matchForm, Map<String, Object> targetForm) implements Specification { }

    private static final List<Object> STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, BulkChange.Prop.bulkChangeStatus.toString());
    private static final List<Object> LABELS_PATH = List.of(JsonLd.GRAPH_KEY, 1, BulkChange.Prop.label.toString(), "*");
    private static final List<Object> COMMENTS_PATH = List.of(JsonLd.GRAPH_KEY, 1, BulkChange.Prop.comment.toString(), "*");
    private static final List<Object> SPECIFICATION_PATH = List.of(JsonLd.GRAPH_KEY, 1, BulkChange.Prop.bulkChangeSpecification.toString());

    public BulkChangeDocument(Map<?, ?> data) {
        super(data);

        if (!BulkChange.Type.BulkChange.toString().equals(getThingType())) {
            throw new IllegalArgumentException("Document is not a " + BulkChange.Type.BulkChange);
        }
    }

    public BulkChange.Status getStatus() {
        return BulkChange.Status.valueOf(get(STATUS_PATH, null));
    }

    public void setStatus(BulkChange.Status status) {
        _set(STATUS_PATH, status.toString(), data);
    }

    public List<String> getLabels() {
        return get(LABELS_PATH, Collections.emptyList());
    }

    public List<String> getComments() {
        return get(COMMENTS_PATH, Collections.emptyList());
    }

    // TODO
    public boolean isLoud() {
        return false;
    }

    public Specification getSpecification() {
        Map<String, Object> spec = get(SPECIFICATION_PATH, null);
        if (spec == null) {
            throw new IllegalArgumentException("Nothing in " + SPECIFICATION_PATH);
        }

        if (BulkChange.Type.FormSpecification.toString().equals(spec.get(JsonLd.TYPE_KEY))) {
            return new FormSpecification(
                    get(spec, List.of(BulkChange.Prop.matchForm.toString()), Collections.emptyMap()),
                    get(spec, List.of(BulkChange.Prop.targetForm.toString()), Collections.emptyMap())
            );
        }

        throw new IllegalArgumentException(String.format("Bad %s: %s", BulkChange.Prop.bulkChangeSpecification, spec));
    }

    private <T> T get(List<Object> path, T defaultTo) {
        return get(data, path, defaultTo);
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Object thing, List<Object> path, T defaultTo) {
        return (T) DocumentUtil.getAtPath(thing, path, defaultTo);
    }
}

package whelk.datatool.bulkchange;

import whelk.Document;
import whelk.JsonLd;

import java.util.List;
import java.util.Map;

class BulkChangeDocument extends Document {

    private static final List STATUS_PATH = List.of(JsonLd.GRAPH_KEY, 1, BulkChange.Prop.bulkChangeStatus.toString());

    public BulkChangeDocument(Map data) {
        super(data);

        if (!BulkChange.Type.BulkChange.toString().equals(getThingType())) {
            throw new IllegalArgumentException("Document is not a " + BulkChange.Type.BulkChange);
        }
    }

    public BulkChange.Status getStatus() {
        return BulkChange.Status.valueOf((String) _get(STATUS_PATH, data));
    }

    public void setStatus(BulkChange.Status status) {
        _set(STATUS_PATH, status.toString(), data);
    }
}

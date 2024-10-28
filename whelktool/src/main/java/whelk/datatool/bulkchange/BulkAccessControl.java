package whelk.datatool.bulkchange;

import whelk.datatool.util.DocumentComparator;
import whelk.Document;
import whelk.exception.ModelValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.datatool.bulkchange.Bulk.Other.changeSpec;
import static whelk.datatool.bulkchange.Bulk.Other.shouldUpdateModifiedTimestamp;
import static whelk.datatool.bulkchange.Bulk.Status.Completed;
import static whelk.datatool.bulkchange.Bulk.Status.Draft;
import static whelk.datatool.bulkchange.Bulk.Status.Failed;
import static whelk.datatool.bulkchange.Bulk.Status.Ready;

public class BulkAccessControl {
    private static final Map<Bulk.Status, List<Bulk.Status>> VALID_STATE_TRANSITIONS_BY_USER = Map.of(
            Draft, List.of(Draft, Ready),
            Completed, List.of(Ready),
            Failed, List.of(Ready)
    );

    public static void verify(Document _oldDoc, Document _newDoc) {
        var oldDoc = new BulkJobDocument(_oldDoc);
        var newDoc = new BulkJobDocument(_newDoc);
        checkTransitionByUser(oldDoc, newDoc);
    }

    public static void checkTransitionByUser(BulkJobDocument oldDoc, BulkJobDocument newDoc) {
        if (!VALID_STATE_TRANSITIONS_BY_USER.getOrDefault(oldDoc.getStatus(), Collections.emptyList()).contains(newDoc.getStatus())) {
            var msg = String.format("Cannot go from %s to %s", oldDoc.getStatus(), newDoc.getStatus());
            throw new ModelValidationException(msg);
        }

        if (newDoc.getStatus() != Draft) {
            var isSameSpec = new DocumentComparator().isEqual(newDoc.getSpecificationRaw(), oldDoc.getSpecificationRaw());
            if (!isSameSpec) {
                var msg = String.format("Cannot change %s when not a %s", changeSpec, Draft);
                throw new ModelValidationException(msg);
            }
        }

        if (oldDoc.getStatus() != Draft && newDoc.shouldUpdateModifiedTimestamp() != oldDoc.shouldUpdateModifiedTimestamp()) {
            var msg = String.format("Cannot change %s when already ran", shouldUpdateModifiedTimestamp);
            throw new ModelValidationException(msg);
        }
    }
}

package whelk.datatool.bulkchange;

import whelk.datatool.util.DocumentComparator;
import whelk.Document;
import whelk.exception.ModelValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BulkAccessControl {
    private static final Map<BulkChange.Status, List<BulkChange.Status>> VALID_STATE_TRANSITIONS_BY_USER = Map.of(
            BulkChange.Status.DraftBulkChange, List.of(BulkChange.Status.DraftBulkChange, BulkChange.Status.ReadyBulkChange),
            BulkChange.Status.CompletedBulkChange, List.of(BulkChange.Status.ReadyBulkChange),
            BulkChange.Status.FailedBulkChange, List.of(BulkChange.Status.ReadyBulkChange)
    );

    public static void verify(Document _oldDoc, Document _newDoc) {
        var oldDoc = new BulkChangeDocument(_oldDoc);
        var newDoc = new BulkChangeDocument(_newDoc);
        checkTransitionByUser(oldDoc, newDoc);
    }

    public static void checkTransitionByUser(BulkChangeDocument oldDoc, BulkChangeDocument newDoc) {
        if (!VALID_STATE_TRANSITIONS_BY_USER.getOrDefault(oldDoc.getStatus(), Collections.emptyList()).contains(newDoc.getStatus())) {
            var msg = String.format("Cannot go from %s to %s", oldDoc.getStatus(), newDoc.getStatus());
            throw new ModelValidationException(msg);
        }

        if (newDoc.getStatus() != BulkChange.Status.DraftBulkChange) {
            var isSameSpec = new DocumentComparator().isEqual(newDoc.getSpecificationRaw(), oldDoc.getSpecificationRaw());
            if (!isSameSpec) {
                var msg = String.format("Cannot change %s when not a %s", BulkChange.Prop.bulkChangeSpecification, BulkChange.Status.DraftBulkChange);
                throw new ModelValidationException(msg);
            }
        }
    }
}

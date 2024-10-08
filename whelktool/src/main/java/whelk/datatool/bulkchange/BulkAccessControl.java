package whelk.datatool.bulkchange;

import whelk.datatool.util.DocumentComparator;
import whelk.Document;
import whelk.exception.ModelValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static whelk.datatool.bulkchange.BulkChange.Prop.bulkChangeMetaChanges;
import static whelk.datatool.bulkchange.BulkChange.Prop.bulkChangeSpecification;
import static whelk.datatool.bulkchange.BulkChange.Status.CompletedBulkChange;
import static whelk.datatool.bulkchange.BulkChange.Status.DraftBulkChange;
import static whelk.datatool.bulkchange.BulkChange.Status.FailedBulkChange;
import static whelk.datatool.bulkchange.BulkChange.Status.ReadyBulkChange;

public class BulkAccessControl {
    private static final Map<BulkChange.Status, List<BulkChange.Status>> VALID_STATE_TRANSITIONS_BY_USER = Map.of(
            DraftBulkChange, List.of(DraftBulkChange, ReadyBulkChange),
            CompletedBulkChange, List.of(ReadyBulkChange),
            FailedBulkChange, List.of(ReadyBulkChange)
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

        if (newDoc.getStatus() != DraftBulkChange) {
            var isSameSpec = new DocumentComparator().isEqual(newDoc.getSpecificationRaw(), oldDoc.getSpecificationRaw());
            if (!isSameSpec) {
                var msg = String.format("Cannot change %s when not a %s", bulkChangeSpecification, DraftBulkChange);
                throw new ModelValidationException(msg);
            }
        }

        if (oldDoc.getStatus() != DraftBulkChange && newDoc.isLoud() != oldDoc.isLoud()) {
            var msg = String.format("Cannot change %s when already ran", bulkChangeMetaChanges);
            throw new ModelValidationException(msg);
        }
    }
}

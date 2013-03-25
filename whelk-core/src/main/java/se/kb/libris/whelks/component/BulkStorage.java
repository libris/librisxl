package se.kb.libris.whelks.component;

import se.kb.libris.whelks.Document;

/**
 * Storage capable of storing documents in bulk.
 */
public interface BulkStorage extends Storage {
    public void bulkStore(Iterable<Document> documents, String whelkPrefix);
}

package se.kb.libris.whelks.component;

import java.util.List;
import java.net.URI;
import se.kb.libris.whelks.Document;

public interface BatchGraphStore extends GraphStore {
    public int getOptimumBatchSize();
    void batchUpdate(List<Document> batch);
}

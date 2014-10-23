package whelk.component;

import java.util.List;
import java.net.URI;
import whelk.Document;

public interface BatchGraphStore extends GraphStore {
    public int getOptimumBatchSize();
    void batchUpdate(List<Document> batch);
}

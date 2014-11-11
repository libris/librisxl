package whelk.component;

import java.util.List;
import whelk.Document;

public interface BatchGraphStore extends GraphStore {
    public int getOptimumBatchSize();
    void batchUpdate(List<Document> batch);
}

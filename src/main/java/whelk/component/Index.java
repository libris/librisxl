package whelk.component;

import whelk.Document;

import java.util.List;

/**
 * Created by markus on 15-09-17.
 */
public interface Index {
    void index(Document document);
    void bulkIndex(List<Document> documents);
}

package whelk.component;

import whelk.Document;

import java.util.List;
import java.util.Map;

/**
 * Created by markus on 15-09-17.
 */
public interface Index {
    void index(Document document, String collection);
    void bulkIndex(List<Document> documents, String collection);
    void remove(String id);
    Map query(Map dslQuery, String dataset);
}

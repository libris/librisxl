package whelk.component;

import whelk.Document;
import whelk.Location;

import java.util.List;

/**
 * Created by markus on 15-09-18.
 */
public interface Storage {
    Document store(Document document);
    void bulkStore(List<Document> documents);
    Location locate(String uri);
    Document load(String id);
    Iterable<Document> loadAll(String dataset);
}

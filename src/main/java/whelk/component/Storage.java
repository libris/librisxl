package whelk.component;

import whelk.Document;
import whelk.Location;

import java.util.List;
import java.util.Map;

/**
 * Created by markus on 15-09-18.
 */
public interface Storage {
    Document store(Document document);
    boolean bulkStore(List<Document> documents);
    Location locate(String uri, boolean loadDocumentIfFound);
    Document load(String id);
    Iterable<Document> loadAll(String dataset);
    boolean remove(String id, String dataset);
    Map<String,Object> query(Map<String,String[]> queryParameters, String dataset, StorageType storageType);
}

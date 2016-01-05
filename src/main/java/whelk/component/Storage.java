package whelk.component;

import whelk.Document;
import whelk.Location;

import java.util.List;
import java.util.Map;

/**
 * Created by markus on 15-09-18.
 */
public interface Storage {
    Document store(Document document, boolean createOrUpdate);
    boolean bulkStore(List<Document> documents, boolean createOrUpdate);
    Location locate(String uri, boolean loadDocumentIfFound);
    Document load(String id);
    List<String> loadCollections();
    Iterable<Document> loadAll(String dataset);
    boolean remove(String id);
    void setVersioning(boolean v);
    boolean getVersioning();
    Map<String,Object> query(Map<String,String[]> queryParameters, String dataset, StorageType storageType);
    Map<String,Object> loadSettings(String key);
    void saveSettings(String key, Map<String,Object> settings);
}

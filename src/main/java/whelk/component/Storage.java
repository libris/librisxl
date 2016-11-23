package whelk.component;

import whelk.Document;
import whelk.Location;

import java.util.List;
import java.util.Map;

/**
 * Created by markus on 15-09-18.
 */
public interface Storage {
    boolean store(Document document, boolean createOrUpdate, String changedIn, String changedBy, String collection, boolean deleted);
    boolean bulkStore(List<Document> documents, boolean createOrUpdate, String changedIn, String changedBy, String collection);
    Location locate(String uri, boolean loadDocumentIfFound);
    Document load(String id);
    List<String> loadCollections();
    Iterable<Document> loadAll(String dataset);
    boolean remove(String id, String changedIn, String changedBy, String collection);
    void setVersioning(boolean v);
    boolean getVersioning();
    Map<String,Object> query(Map<String,String[]> queryParameters, String dataset, StorageType storageType);
    Map<String,Object> loadSettings(String key);
    void saveSettings(String key, Map<String,Object> settings);
    List<Document> findByRelation(String relation, String reference,
                                  int limit, int offset);
    List<Document> findByQuotation(String identifier, int limit, int offset);
    List<Document> findByValue(String relation, String value,
                               int limit, int offset);
    int countByRelation(String relation, String reference);
    int countByQuotation(String identifier);
    int countByValue(String relation, String value);
}

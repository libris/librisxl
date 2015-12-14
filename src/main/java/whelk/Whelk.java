package whelk;

import java.io.InputStream;
import java.util.*;
import whelk.plugin.Plugin;
import whelk.result.SearchResult;
import whelk.result.SparqlResult;
import org.apache.camel.CamelContext;

public interface Whelk {

    // constants
    static final String ADD_OPERATION = "ADD";
    static final String BULK_ADD_OPERATION = "BULK_ADD";
    static final String REMOVE_OPERATION = "DELETE";

    /*
    // storage
    public String add(Document d);
    public void bulkAdd(List<Document> d, String dataset, String contentType);
    public void remove(String identifier);
    public void remove(String identifier, String dataset);

    // get/search/lookup
    public Document get(String identifier);
    public SearchResult search(Query query);
    public Iterable<Document> loadAll(String dataset);
    public Location locate(String identifier);
    public Map<String, String> getVersions(String identifier);

    public InputStream sparql(String query);

    // Document creation
    public Document createDocument(String contentType);
    public Document createDocumentFromJson(String jsonData);
    public Document createDocument(byte[] data, Map documentEntry, Map documentMeta);
    public Document createDocument(Map data, Map documentEntry, Map documentMeta);

    // maintenance
    public String getId(); // Whelk ID
    public void addPlugin(Plugin plugin);
    public void flush();
    public String mintIdentifier(Document document);
    public boolean acquireLock(String dataset);
    public void releaseLock(String dataset);
    public boolean updateState(String key, Map data);

    // ecosystem
    public void init();
    public Map getProps();
    public void setProps(Map props);
    public CamelContext getCamelContext();
    public void notifyCamel(Document doc, String operation, Map extraInfo);
    public void notifyCamel(String identifier, String dataset, String operation, Map extraInfo);
    */
}

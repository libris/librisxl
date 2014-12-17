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

    // storage
    public String add(Document d);
    /*
    public String add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata);
            */
    public void bulkAdd(List<Document> d, String contentType);
    public Document get(String identifier);
    public void remove(String identifier);
    public Map<String, String> getVersions(String identifier);

    // search/lookup
    public SearchResult search(Query query);
    public Iterable<Document> loadAll();
    public Iterable<Document> loadAll(Date since);
    public Iterable<Document> loadAll(String dataset);
    public Iterable<Document> loadAll(String dataset, Date since);
    public Location locate(String identifier);

    public InputStream sparql(String query);

    // maintenance
    public String getId(); // Whelk ID
    public void addPlugin(Plugin plugin);
    public void flush();
    public String mintIdentifier(Document document);

    // ecosystem
    public Map getGlobal();
    public CamelContext getCamelContext();
    public void notifyCamel(String identifier, String dataset, String operation, Map extraInfo);
}

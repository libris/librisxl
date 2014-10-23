package whelk;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import whelk.plugin.Plugin;
import whelk.result.SearchResult;
import whelk.result.SparqlResult;
import org.apache.camel.CamelContext;

public interface Whelk {

    // constants
    static final String ADD_OPERATION = "ADD";
    static final String REMOVE_OPERATION = "DELETE";

    // storage
    public URI add(Document d);
    public URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata);
    public void bulkAdd(List<Document> d, String contentType);
    public Document get(URI identifier);
    public void remove(URI identifier);

    // search/lookup
    public SearchResult search(Query query);
    public Iterable<Document> loadAll();
    public Iterable<Document> loadAll(Date since);
    public Iterable<Document> loadAll(String dataset);
    public Iterable<Document> loadAll(String dataset, Date since);
    public Location locate(URI identifier);

    public InputStream sparql(String query);

    // maintenance
    public String getId(); // Whelk ID
    public void addPlugin(Plugin plugin);
    public void flush();
    public URI mintIdentifier(Document document);

    // ecosystem
    public Map getGlobal();
    public CamelContext getCamelContext();
    public void notifyCamel(String identifier, String operation, Map extraInfo);
}

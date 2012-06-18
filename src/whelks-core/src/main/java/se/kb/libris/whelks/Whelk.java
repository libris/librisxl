package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.*;

public interface Whelk {
    // storage
    public URI store(Document d);
    public Document get(URI identifier);
    public void delete(URI identifier);
    
    // search/lookup
    public SearchResult<? extends Document> query(String query);
    public SearchResult<? extends Document> query(Query query);

    public LookupResult<? extends Document> lookup(Key key);
    public SparqlResult sparql(String query);

    // maintenance
    public void destroy();
    public Iterable<LogEntry> log(int startIndex);
    public Iterable<LogEntry> log(URI identifier);
    public String getName();
    public void setManager(WhelkManager manager);
    public WhelkManager getManager();
    
    // factory methods
    public Document createDocument();

    // notifications
    public void notify(URI uri);
}

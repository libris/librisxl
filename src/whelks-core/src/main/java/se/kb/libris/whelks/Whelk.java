package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.plugin.Plugin;

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
    public Iterable<LogEntry> log(Date since);
    public String getPrefix();
    public Iterable<? extends Plugin> getPlugins();
    
    // factory methods
    public Document createDocument();

    // notifications
    public void notify(URI uri);
}

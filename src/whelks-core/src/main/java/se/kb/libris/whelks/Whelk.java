package se.kb.libris.whelks;

import java.net.URI;

public interface Whelk {
    // storage
    public URI store(Document d);
    public Document get(URI identifier);
    public void delete(URI identifier);
    
    // search/lookup
    public SearchResult<? extends Document> query(String query);
    public LookupResult<? extends Document> lookup(Key key);

    // maintenance
    public String getPrefix();
    public void destroy();
    public Iterable<LogEntry> log(int startIndex);
    public Iterable<LogEntry> log(URI identifier);
    
    // factory methods
    public Document createDocument();
}

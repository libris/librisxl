package se.kb.libris.whelks;

import java.net.URI;

public interface Whelk {
    // storage
    public URI store(Document d);
    public Document get(URI uri);
    public void delete(URI uri);
    
    // search/lookup
    public SearchResult query(QueryType type, String query);
    public LookupResult<? extends Document> lookup(Key key);

    // maintenance
    public Iterable<LogEntry> log(int startIndex);
    public void destroy();
    
    // factory methods
    public Document createDocument(String contentType, byte[] data);
    //public Document createDocument(String contentType, ByteArraInputStream data);
}

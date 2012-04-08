package se.kb.libris.whelks;

import java.net.URI;

public interface Whelk {
    // storage
    public URI store(Document d);
    public URI store(URI uri, Document d);
    public Document get(URI uri);
    public void delete(URI uri);
    
    // search/lookup
    public SearchResult<? extends Document> query(String query);
    public LookupResult<? extends Document> lookup(Key key);

    // maintenance
    public void destroy();
    
    // factory methods
    public Document createDocument(String contentType, String format, byte[] data);
}

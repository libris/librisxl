package se.kb.libris.whelks;

import java.net.URI;

public interface Whelk {
    // storage
    public URI store(Document d);
    public Document get(URI uri);
    public void delete(URI uri);
    
    // search/lookup/browse
    public SearchResult<? extends Document> find(String query);
    public LookupResult<? extends Document> lookup(Key key);
    public BrowseResult<? extends Key> browse(URI type, String start);

    // factory methods
    public Document createDocument();
}

package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.plugin.Plugin;

public interface Whelk {
    // storage
    public URI store(Document d);
    public void bulkStore(Iterable<Document> d);
    public Document get(URI identifier);
    public void delete(URI identifier);

    // search/lookup
    public SearchResult<? extends Document> query(String query);
    public SearchResult<? extends Document> query(Query query);

    public LookupResult<? extends Document> lookup(Key key);
    public SparqlResult sparql(String query);

    // maintenance
    public void init();
    public void destroy();
    public Iterable<Document> log();
    public Iterable<Document> log(int startIndex);
    public Iterable<Document> log(URI identifier);
    public Iterable<Document> log(Date since);
    public String getPrefix();
    public Iterable<? extends Plugin> getPlugins();
    public void reindex();

    // factory methods
    public Document createDocument(byte[] data, Map<String, Object> metadata);
}

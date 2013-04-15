package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.plugin.Plugin;

public interface Whelk {
    // storage
    public URI store(Document d);
    public void bulkStore(List<Document> d);
    public Document get(URI identifier);
    public void delete(URI identifier);

    // search/lookup
    //public SearchResult<? extends Document> query(String query);
    public SearchResult<? extends Document> query(Query query);
    public Iterable<Document> loadAll();
    public Iterable<Document> loadAll(Date since);

    /* Maybe implement later
    public SparqlResult sparql(String query);
    */

    // maintenance
    public void reindex();

    public String getPrefix();
    public void addPlugin(Plugin plugin);
    public Iterable<? extends Plugin> getPlugins();

    // factory methods
    public Document createDocument(byte[] data, Map<String, Object> metadata);
}

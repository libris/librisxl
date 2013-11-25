package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.plugin.Plugin;

public interface Whelk {
    // storage
    public URI add(Document d);
    public URI add(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata);
    public void bulkAdd(List<Document> d);
    public Document get(URI identifier);
    public void remove(URI identifier);

    // search/lookup
    public SearchResult search(Query query);
    public Iterable<Document> loadAll();
    public Iterable<Document> loadAll(Date since);

    /* implement later
    public SparqlResult sparql(String query);
    */

    // maintenance
    public void reindex();
    public String getId();
    public void addPlugin(Plugin plugin);
    public Iterable<? extends Plugin> getPlugins();
    public void flush();

    // factory methods
    public Document createDocument(byte[] data,
            Map<String, Object> entrydata,
            Map<String, Object> metadata);
    public URI mintIdentifier(Document document);
}

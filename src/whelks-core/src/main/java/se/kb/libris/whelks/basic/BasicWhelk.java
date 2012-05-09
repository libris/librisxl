package se.kb.libris.whelks.basic;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;

public class BasicWhelk implements Whelk, Pluggable {
    private List<Plugin> plugins = new LinkedList<Plugin>();

    public URI store(Document d) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document get(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public SearchResult query(String query, QueryType type) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public LookupResult<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<LogEntry> log(int startIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document createDocument(String contentType, byte[] data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public Document createDocument(URI identifier, String contentType, byte[] data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<LogEntry> log(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<Plugin> getPlugins() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void addPlugin(Plugin plugin) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void removePlugin(String id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}

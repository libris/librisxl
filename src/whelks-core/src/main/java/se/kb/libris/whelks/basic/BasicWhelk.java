package se.kb.libris.whelks.basic;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;

public class BasicWhelk implements Whelk, Pluggable, JSONInitialisable {
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

    public SearchResult query(QueryType type, String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public LookupResult<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<LogEntry> log(int startIndex) {
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

    public JSONInitialisable init(JSONObject obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document createDocument(String contentType, byte[] data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document createDocument(String contentType, ByteArrayInputStream data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}

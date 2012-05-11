package se.kb.libris.whelks.test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.persistance.JSONSerialisable;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;

public class TestWhelk implements Whelk, Pluggable, JSONSerialisable {
    List<Plugin> plugins = new LinkedList<Plugin>();
    
    public TestWhelk() {
    }
    
    public URI store(Document d) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document get(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void delete(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public SearchResult<? extends Document> query(String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public LookupResult<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<LogEntry> log(int startIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<LogEntry> log(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document createDocument(String contentType, byte[] data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Document createDocument(String contentType, ByteArrayInputStream data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterable<? extends Plugin> getPlugins() {
        return plugins;
    }

    public void addPlugin(Plugin plugin) {
        plugins.add(plugin);
    }

    public void removePlugin(String id) {
    }

    public JSONObject serialize() {
        JSONObject o = new JSONObject();
        o.put("test", "test");
                
        return o;
    }
}

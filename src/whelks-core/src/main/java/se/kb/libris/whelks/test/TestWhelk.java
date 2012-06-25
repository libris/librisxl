package se.kb.libris.whelks.test;

import java.net.URI;
import java.util.LinkedList;
import java.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.persistance.JSONSerialisable;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;

public class TestWhelk implements Whelk, Pluggable, JSONSerialisable, JSONInitialisable {
    List<Plugin> plugins = new LinkedList<Plugin>();
    
    public TestWhelk() {
    }
    
    @Override
    public URI store(Document d) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document get(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult<? extends Document> query(String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult query(Query query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void notify(URI u) {}

    @Override
    public LookupResult<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<LogEntry> log(int startIndex) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<LogEntry> log(URI identifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document createDocument() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<? extends Plugin> getPlugins() {
        return plugins;
    }

    @Override
    public void addPlugin(Plugin plugin) {
        plugins.add(plugin);
    }

    @Override
    public void removePlugin(String id) {
    }

    @Override
    public JSONObject serialize() {
        JSONObject o = new JSONObject();
        o.put("_classname", this.getClass().getName());
        o.put("test", "test");
        
        JSONArray _plugins = new JSONArray();
        for (Plugin p: plugins) {
            JSONObject _plugin = (p instanceof JSONSerialisable)? ((JSONSerialisable)p).serialize():new JSONObject();
            _plugins.add(_plugin);
                    
        }
        o.put("plugins", _plugins);
                
        return o;
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public WhelkManager getManager() {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public SparqlResult sparql(String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setManager(WhelkManager wm) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public String getPrefix() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public void setPrefix(String p) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}

package se.kb.libris.whelks.basic;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.component.Component;
import se.kb.libris.whelks.component.Index;
import se.kb.libris.whelks.component.QuadStore;
import se.kb.libris.whelks.component.Storage;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.plugin.*;

public class BasicWhelk implements Whelk, Pluggable, JSONInitialisable {
    private List<Plugin> plugins = new LinkedList<Plugin>();

    public URI store(Document d) {
        /**
         * @todo if storage becomes optional, minting URIs needs to happen somewhere else (which is probably just as good anyway)
         * if (d.getIdentifier() == null)
         *  ...
         */
        
        // before triggers
        for (Trigger t: getTriggers())
            t.beforeStore(this, d);
        
        // add document to store, index and quadstore
        for (Component c: getComponents())
            if (c instanceof Storage)
                ((Storage)c).store(d);
            else if (c instanceof Index)
                ((Index)c).index(d);
            else if (c instanceof QuadStore)
                ((QuadStore)c).update(d.getIdentifier(), d);        

        // after triggers
        for (Trigger t: getTriggers())
            t.afterStore(this, d);
        
        return d.getIdentifier();
    }

    public Document get(URI uri) {
        Document d = null;
        
        for (Component c: getComponents()) {
            if (c instanceof Storage) {
                d = ((Storage)c).get(uri);
                
                if (d != null)
                    return d;
            }
        }
                
        return d;
    }

    public void delete(URI uri) {
        for (Component c: getComponents())
            if (c instanceof Storage)
                ((Storage)c).delete(uri);
            else if (c instanceof Index)
                ((Index)c).delete(uri);
            else if (c instanceof QuadStore)
                ((QuadStore)c).delete(uri);        
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

    public Document convert(Document doc, String mimeType, String format, String profile) {
        for (FormatConverter converter: getPlugins()
    }

    private List<KeyGenerator> getKeyGenerators() {
        synchronized (plugins) {
            List<KeyGenerator> ret = new LinkedList<KeyGenerator>();

            for (Plugin plugin: plugins)
                if (plugin instanceof KeyGenerator)
                    ret.add((KeyGenerator)plugin);

            return ret;
        }
    }

    private List<LinkFinder> getLinkFinders() {
        synchronized (plugins) {
            List<LinkFinder> ret = new LinkedList<LinkFinder>();

            for (Plugin plugin: plugins)
                if (plugin instanceof LinkFinder)
                    ret.add((LinkFinder)plugin);

            return ret;
        }
    }
    
    private List<Trigger> getTriggers() {
        synchronized (plugins) {
            List<Trigger> ret = new LinkedList<Trigger>();

            for (Plugin plugin: plugins)
                if (plugin instanceof Trigger)
                    ret.add((Trigger)plugin);

            return ret;
        }
    }

    private Iterable<Component> getComponents() {
        synchronized (plugins) {
            List<Component> ret = new LinkedList<Component>();

            for (Plugin plugin: plugins)
                if (plugin instanceof Component)
                    ret.add((Component)plugin);

            return ret;
        }
    }

    @Override
    public void addPlugin(Plugin plugin) {
        synchronized (plugins) {
            plugins.add(plugin);
        }
    }

    @Override
    public void removePlugin(String id) {
        synchronized (plugins) {
            ListIterator<Plugin> li = plugins.listIterator();

            while (li.hasNext()) {
                Plugin p = li.next();

                if (p.getId().equals(id))
                    li.remove();
            }
        }
    }
}

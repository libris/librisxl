package se.kb.libris.whelks.basic;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.component.Component;
import se.kb.libris.whelks.component.Index;
import se.kb.libris.whelks.component.QuadStore;
import se.kb.libris.whelks.component.Storage;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.persistance.JSONSerialisable;
import se.kb.libris.whelks.plugin.*;

public class BasicWhelk implements Whelk, Pluggable, JSONInitialisable, JSONSerialisable {
    private List<Plugin> plugins = new LinkedList<Plugin>();
    private WhelkManager manager = null;


    @Override
    public URI store(Document d) {

        /**
         * @todo if storage becomes optional, minting URIs needs to happen somewhere else (which is probably just as good anyway)
         * @todo find links and generate keys to store in document
         */        
        
        // before triggers
        for (Trigger t: getTriggers())
            t.beforeStore(d);
        
        // add document to store
        /*
        OutputStream combinedOutputStream = null;
        for (Storage s : getStorages()) {
            OutputStream os = s.getOutputStreamFor(d);
            if (combinedOutputStream == null) {
                combinedOutputStream = os;
            } else {
                combinedOutputStream = new TeeOutputStream(combinedOutputStream, os);
            }
        }
        if (combinedOutputStream != null) {
            try {
                long savedBytes = IOUtils.copyLarge(d.getDataAsStream(), combinedOutputStream);
                if (d.getSize() != savedBytes) {
                    throw new WhelkRuntimeException("Expected "+d.getSize() +" bytes. Received "+savedBytes+".");
                } 
            } catch (Exception e) {
                throw new WhelkRuntimeException(e);
            } finally {
                try {
                    combinedOutputStream.close();
                } catch (IOException ioe) {
                    throw new WhelkRuntimeException(ioe);
                }
            }
        }
        */

        // add document to index and quadstore
        for (Component c: getComponents()) {
            if (c instanceof Storage) 
                ((Storage)c).store(d);

            if (c instanceof Index)
                ((Index)c).index(d);

            if (c instanceof QuadStore)
                ((QuadStore)c).update(d.getIdentifier(), d);
        }

        // after triggers
        for (Trigger t: getTriggers()) {
            t.afterStore(d);
        }

        System.out.println("Stored record with URI " + d.getIdentifier());
        
        return d.getIdentifier();
    }


    @Override
    public Document get(URI uri) {
        Document d = null;

        for (Component c: getComponents()) {
            if (c instanceof Storage) {
                d = ((Storage)c).get(uri);
                
                if (d != null) {
                    return d;
                }
            }
        }
        return d;
    }

    @Override
    public void delete(URI uri) {
        // before triggers
        for (Trigger t: getTriggers())
            t.beforeDelete(uri);
        
        for (Component c: getComponents())
            if (c instanceof Storage)
                ((Storage)c).delete(uri);
            else if (c instanceof Index)
                ((Index)c).delete(uri);
            else if (c instanceof QuadStore)
                ((QuadStore)c).delete(uri);        

        // after triggers
        for (Trigger t: getTriggers())
            t.afterDelete(uri);
    }

    @Override
    public SearchResult query(String query, LinkedHashMap<String,String> sort, Collection<String> highlight) {
        for (Component c: getComponents())
            if (c instanceof Index)
                return ((Index)c).query(query, sort, highlight);
        
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    @Override
    public LookupResult<? extends Document> lookup(Key key) {
        for (Component c: getComponents())
            if (c instanceof Storage)
                return ((Storage)c).lookup(key);

        throw new WhelkRuntimeException("Whelk has no index for searching");
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
    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document createDocument() {
        return new BasicDocument();
    }

    @Override
    public String getName() {
        if (getManager() != null) {
            return getManager().whoami(this);
        }
        return "";
    }

    @Override
    public WhelkManager getManager() {
        return this.manager;
    }

    @Override
    public void setManager(WhelkManager wm) {
        this.manager = wm;
    }

    public Document convert(Document doc, String mimeType, String format, String profile) {
        return null;
        //for (FormatConverter converter: getPlugins()
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

    @Override
    public void notify(URI u) {}

    protected Iterable<Storage> getStorages() {
        synchronized (plugins) {
            List<Storage> ret = new LinkedList<Storage>();

            for (Plugin plugin: plugins)
                if (plugin instanceof Storage)
                    ret.add((Storage)plugin);

            return ret;
        }
    }

    protected Iterable<Component> getComponents() {
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
            plugin.setWhelk(this);
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

    @Override
    public Iterable<? extends Plugin> getPlugins() {
        return plugins;
    }

    @Override
    public JSONInitialisable init(JSONObject obj) {
        for (Iterator it = ((JSONArray)obj.get("plugins")).iterator(); it.hasNext();) {
            try {
                JSONObject _plugin = (JSONObject)it.next();
                Class c = Class.forName(_plugin.get("_classname").toString());
                
                Plugin p = (Plugin)c.newInstance();
                if (JSONInitialisable.class.isAssignableFrom(c))
                    ((JSONInitialisable)p).init(_plugin);
                
                p.setWhelk(this);
                plugins.add(p);
            } catch (Exception e) {
                throw new WhelkRuntimeException(e);
            }
        }
        
        return this;
    }

    @Override
    public JSONObject serialize() {
        JSONObject _whelk = new JSONObject();
        _whelk.put("_classname", this.getClass().getName());
        
        JSONArray _plugins = new JSONArray();
        for (Plugin p: plugins) {
            JSONObject _plugin = (p instanceof JSONSerialisable)? ((JSONSerialisable)p).serialize():new JSONObject();
            _plugin.put("_classname", p.getClass().getName());
            _plugins.add(_plugin);
                    
        }
        _whelk.put("plugins", _plugins);
                
        return _whelk;
    }
}

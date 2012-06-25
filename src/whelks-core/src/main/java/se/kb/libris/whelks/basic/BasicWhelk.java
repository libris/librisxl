package se.kb.libris.whelks.basic;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.api.*;
import se.kb.libris.whelks.component.*;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.persistance.JSONSerialisable;
import se.kb.libris.whelks.plugin.*;

public class BasicWhelk implements Whelk, Pluggable, JSONInitialisable, JSONSerialisable {
    private Random random = new Random();
    private final List<Plugin> plugins = new LinkedList<Plugin>();
    private String prefix = null;
    private WhelkManager manager = null;

    public BasicWhelk() {}

    public BasicWhelk(String pfx) {
        setPrefix(pfx);
    }

    public String getPrefix() { return this.prefix; }

    public void setPrefix(String pfx) { 
        if (pfx != null && pfx.startsWith("/")) {
            pfx = pfx.substring(1);
        }
        this.prefix = pfx; 
    }

    @Override
    public URI store(Document d) {
        // mint URI if document needs it
        if (d.getIdentifier() == null || !d.getIdentifier().toString().startsWith("/"+prefix))
            d.setIdentifier(mintIdentifier(d));
        
        // find and add links
        d.getLinks().clear();
        for (LinkFinder lf: getLinkFinders())
            d.getLinks().addAll(lf.findLinks(d));
                
        // generate and add keys
        d.getKeys().clear();
        for (KeyGenerator kg: getKeyGenerators())
            d.getKeys().addAll(kg.generateKeys(d));
        
        // extract descriptions
        d.getDescriptions().clear();
        for (DescriptionExtractor de: getDescriptionExtractors())
            d.getDescriptions().add(de.extractDescription(d));
        
        // before triggers
        for (Trigger t: getTriggers())
            t.beforeStore(this, d);
        
        // add document to storage, index and quadstore
        for (Component c: getComponents()) {
            if (c instanceof Storage)
                ((Storage)c).store(d);

            if (c instanceof Index)
                ((Index)c).index(d);

            if (c instanceof QuadStore)
                ((QuadStore)c).update(d.getIdentifier(), d);
        }

        // after triggers
        for (Trigger t: getTriggers())
            t.afterStore(this, d);
        
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
            t.beforeDelete(this, uri);
        
        for (Component c: getComponents())
            if (c instanceof Storage)
                ((Storage)c).delete(uri);
            else if (c instanceof Index)
                ((Index)c).delete(uri);
            else if (c instanceof QuadStore)
                ((QuadStore)c).delete(uri);        

        // after triggers
        for (Trigger t: getTriggers())
            t.afterDelete(this, uri);

    }

    @Override
    public SearchResult query(String query) {
        return query(new Query(query));
    }

    @Override
    public SearchResult query(Query query) {
        for (Component c: getComponents())
            if (c instanceof Index)
                return ((Index)c).query(query);
        
        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    @Override
    public LookupResult<? extends Document> lookup(Key key) {
        for (Component c: getComponents())
            if (c instanceof Storage)
                return ((Storage)c).lookup(key);

        throw new WhelkRuntimeException("Whelk has no storage for searching");
    }

    @Override
    public SparqlResult sparql(String query) {
        for (Component c: getComponents())
            if (c instanceof QuadStore)
                return ((QuadStore)c).sparql(query);

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

    private List<DescriptionExtractor> getDescriptionExtractors() {
        synchronized (plugins) {
            List<DescriptionExtractor> ret = new LinkedList<DescriptionExtractor>();

            for (Plugin plugin: plugins)
                if (plugin instanceof DescriptionExtractor)
                    ret.add((DescriptionExtractor)plugin);

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

    protected Iterable<API> getAPIs() {
        synchronized (plugins) {
            List<API> ret = new LinkedList<API>();

            for (Plugin plugin: plugins)
                if (plugin instanceof API)
                    ret.add((API)plugin);

            return ret;
        }
    }

    @Override
    public void addPlugin(Plugin plugin) {
        synchronized (plugins) {
            if (plugin instanceof Component)
                ((Component)plugin).setWhelk(this);
            if (plugin instanceof API) 
                ((API)plugin).setWhelk(this);
            
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
        try {
            prefix = obj.get("prefix").toString();
        
            for (Iterator it = ((JSONArray)obj.get("plugins")).iterator(); it.hasNext();) {
                    JSONObject _plugin = (JSONObject)it.next();
                    Class c = Class.forName(_plugin.get("_classname").toString());
                
                    Plugin p = (Plugin)c.newInstance();
                    if (JSONInitialisable.class.isAssignableFrom(c))
                        ((JSONInitialisable)p).init(_plugin);
                
                    addPlugin(p);
            }
        } catch (Exception e) {
            throw new WhelkRuntimeException(e);
        }            
        
        return this;
    }

    @Override
    public JSONObject serialize() {
        JSONObject _whelk = new JSONObject();
        _whelk.put("prefix", prefix);
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

    private URI mintIdentifier(Document d) {
        try {
            return new URI("/"+prefix.toString() + UUID.randomUUID());
        } catch (URISyntaxException ex) {
            throw new WhelkRuntimeException("Could not mint URI", ex);
        }

        /*
        for (Plugin p: getPlugins())
            if (p instanceof URIMinter)
                return ((URIMinter)p).mint(d);

        throw new WhelkRuntimeException("No URIMinter found, unable to mint URI");
        */
    }
}

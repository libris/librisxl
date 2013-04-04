package se.kb.libris.whelks.basic;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
/*
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
*/
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.api.*;
import se.kb.libris.whelks.component.*;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
/*
import se.kb.libris.whelks.persistance.JSONInitialisable;
import se.kb.libris.whelks.persistance.JSONSerialisable;
*/
import se.kb.libris.whelks.plugin.*;

@Deprecated
public abstract class BasicWhelk implements Whelk, Pluggable { //, JSONInitialisable, JSONSerialisable {
    private Random random = new Random();
    private final List<Plugin> plugins = new LinkedList<Plugin>();
    private String prefix;

    public BasicWhelk(String pfx) {
        this.prefix = ((pfx != null && pfx.startsWith("/")) ? pfx.substring(1) : pfx);
    }

    @Override
        public String getPrefix() { return this.prefix; }

    @Override
    public URI store(Document d) {
        if (d.getIdentifier() == null || !d.getIdentifier().toString().startsWith("/"+prefix)) {
            d.setIdentifier(mintIdentifier(d));
        }
        List<Document> docs = new ArrayList<Document>();
        docs.add(d);
        bulkStore(docs);
        return d.getIdentifier();
    }

    public void bulkStore(Iterable<Document> docs) {
        // Pre storage operations
        for (Document doc : docs) {
            if (doc.getIdentifier() == null || !doc.getIdentifier().toString().startsWith("/"+prefix)) {
                doc.setIdentifier(mintIdentifier(doc));
            }
            for (Trigger t : getTriggers()) { if (t.isEnabled()) { t.beforeStore(doc); } }
        }

        /*
        for (FormatConverter fc : getFormatConverters()) {
            if (((List)docs).size() > 0 && fc.getRequiredFormat().equals(((Document)((List)docs).get(0)).getFormat())) {
                List<Document> convertedList = new ArrayList<Document>();
                for (Document doc : ((List<Document>)docs)) {
                    convertedList.addAll(fc.convert(doc));
                }
                docs = convertedList;
            }
        }
        */
        for (LinkFinder lf : getLinkFinders()) {
            for (Document doc : docs) {
                for (Link link : lf.findLinks(doc)) {
                    doc.withLink(link);
                }
            }
        }

        if (docs != null && ((List)docs).size() > 0) {
            for (Component c : getComponents()) {
                if (c instanceof BulkStorage) {
                    ((BulkStorage)c).bulkStore(docs, this.prefix);
                }

                if (c instanceof Index) {
                    List<Document> idocs = new ArrayList<Document>();
                    boolean formatConverted = false;
                    for (IndexFormatConverter ifc : getIndexFormatConverters()) {
                        formatConverted = true;
                        for (Document doc : ((List<Document>)docs)) {
                            idocs.addAll(ifc.convert(doc));
                        }
                    }
                    if (!formatConverted) {
                        idocs.addAll((Collection)docs);
                    }
                    for (Document d : idocs) {
                        ((Index)c).index(d, this.prefix);
                    }
                }

                if (c instanceof QuadStore) {
                    for (Document doc : docs) {
                        ((QuadStore)c).update(doc.getIdentifier(), doc);
                    }
                }
            }

            // Post storage operations
            for (Document doc : docs) {
                for (Trigger t : getTriggers()) { if (t.isEnabled()) { t.afterStore(doc); } }
            }
        }
    }

    @Override
    public Document get(URI uri) {
        Document d = null;

        for (Component c: getComponents()) {
            if (c instanceof Storage) {
                d = ((Storage)c).get(uri, this.prefix);

                if (d != null) {
                    Logger.getLogger(this.getClass().getName()).log(Level.FINE, "Document found in storage " + c);
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
                ((Storage)c).delete(uri, this.prefix);
            else if (c instanceof Index)
                ((Index)c).delete(uri, this.prefix);
            else if (c instanceof QuadStore)
                ((QuadStore)c).delete(uri);        

        // after triggers
        for (Trigger t: getTriggers())
            t.afterDelete(uri);

    }

    public SearchResult query(String query) {
        return query(new Query(query));
    }

    @Override
    public SearchResult query(Query query) {
        return query(query, null);
    }

    public SearchResult query(Query query, String indexType) {
        return query(query, this.prefix, indexType);
    }

    public SearchResult query(Query query, String prefix, String indexType) {
        for (Component c: getComponents())
            if (c instanceof Index)
                return ((Index)c).query(query, prefix);

        throw new WhelkRuntimeException("Whelk has no index for searching");
    }

    public SparqlResult sparql(String query) {
        for (Component c: getComponents())
            if (c instanceof QuadStore)
                return ((QuadStore)c).sparql(query);

        throw new WhelkRuntimeException("Whelk has no quadstore component.");
    }

    public Iterable<Document> log() {
        for (Component c: getComponents())
            if (c instanceof Storage)
                return ((Storage)c).getAll(this.prefix);

        throw new WhelkRuntimeException("Whelk has no storage for searching");
    }


    @Override
    public Document createDocument(byte[] data, Map<String, Object> metadata) {
        throw new UnsupportedOperationException("createDocument not supported by "+this.getClass().getName()+".");
    }

    @Override
    public void reindex() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<KeyGenerator> getKeyGenerators() {
        List<KeyGenerator> ret = new LinkedList<KeyGenerator>();

        for (Plugin plugin: plugins)
            if (plugin instanceof KeyGenerator)
                ret.add((KeyGenerator)plugin);

        return ret;
    }

    private List<DescriptionExtractor> getDescriptionExtractors() {
        List<DescriptionExtractor> ret = new LinkedList<DescriptionExtractor>();

        for (Plugin plugin: plugins)
            if (plugin instanceof DescriptionExtractor)
                ret.add((DescriptionExtractor)plugin);

        return ret;
    }

    protected List<LinkFinder> getLinkFinders() {
        List<LinkFinder> ret = new LinkedList<LinkFinder>();

        for (Plugin plugin: plugins)
            if (plugin instanceof LinkFinder)
                ret.add((LinkFinder)plugin);

        return ret;
    }

    private List<Trigger> getTriggers() {
        List<Trigger> ret = new LinkedList<Trigger>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Trigger)
                ret.add((Trigger)plugin);

        return ret;
    }

    protected Iterable<Storage> getStorages() {
        TreeSet<Storage> ret = new TreeSet<Storage>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Storage)
                ret.add((Storage)plugin);

        return ret;
    }

    protected Iterable<Index> getIndexes() {
        TreeSet<Index> ret = new TreeSet<Index>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Index)
                ret.add((Index)plugin);

        return ret;
    }

    protected Iterable<FormatConverter> getFormatConverters() {
        TreeSet<FormatConverter> ret = new TreeSet<FormatConverter>();
        for (Plugin plugin: plugins) 
            if (plugin instanceof FormatConverter)
                ret.add((FormatConverter)plugin);

        return ret;
    }

    protected Iterable<IndexFormatConverter> getIndexFormatConverters() {
        TreeSet<IndexFormatConverter> ret = new TreeSet<IndexFormatConverter>();
        for (Plugin plugin: plugins) 
            if (plugin instanceof IndexFormatConverter)
                ret.add((IndexFormatConverter)plugin);

        return ret;
    }

    protected Iterable<Component> getComponents() {
        TreeSet<Component> ret = new TreeSet<Component>();

        for (Plugin plugin: plugins)
            if (plugin instanceof Component)
                ret.add((Component)plugin);

        return ret;
    }

    protected Iterable<API> getAPIs() {
        List<API> ret = new LinkedList<API>();

        for (Plugin plugin: plugins)
            if (plugin instanceof API)
                ret.add((API)plugin);

        return ret;
    }

    protected void initializePlugins() {
        for (Plugin plugin : plugins) 
            plugin.init(this.prefix);
    }

    @Override
    public void addPlugin(Plugin plugin) {
        synchronized (plugins) {
            if (plugin instanceof WhelkAware) {
                ((WhelkAware)plugin).setWhelk(this);
            }
            plugins.add(plugin);
            initializePlugins();
        }
    }

    @Override
    public void addPluginIfNotExists(Plugin plugin) {
        synchronized (plugins) {
            if (! plugins.contains(plugin)) {
                addPlugin(plugin);
            }
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

    /* Candidate for removal 
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


    @Deprecated
    public void notify(URI u) {}
    */

    protected URI mintIdentifier(Document d) {
        try {
            return new URI("/"+prefix.toString() +"/"+ UUID.randomUUID());
        } catch (URISyntaxException ex) {
            throw new WhelkRuntimeException("Could not mint URI", ex);
        }
    }
}

package se.kb.libris.whelks.backends.basic;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.graph.QuadStore;
import se.kb.libris.whelks.graph.SparqlException;
import se.kb.libris.whelks.index.Index;
import se.kb.libris.whelks.index.Query;
import se.kb.libris.whelks.index.SearchResult;
import se.kb.libris.whelks.plugin.*;
import se.kb.libris.whelks.storage.DocumentStore;

/**
 * @author marma
 */
public class BasicWhelk implements Whelk, Pluggable {
    DocumentStore documentStore = null;
    Index index = null;
    QuadStore quadStore = null;
    private List<Plugin> plugins = new LinkedList<Plugin>();
    
    public BasicWhelk(DocumentStore _documentStore, Index _index, QuadStore _quadStore) {
        documentStore = _documentStore;
        index = _index;
        quadStore = _quadStore;
    }
    
    @Override
    public URI store(Document d) throws WhelkException {
        boolean add = d.getIdentifier() == null;
        
        // extract keys
        for (KeyGenerator kg: getKeyGenerators())
            kg.generateKeys(d);
        
        // find links
        for (LinkFinder lf: getLinkFinders())
            lf.findLinks(d);
        
        // before triggers
        for (Trigger t: getTriggers())
            if (add) t.beforeAdd(this, d);
            else t.beforeUpdate(this, d);
        
        // add document to store
        if (documentStore != null)
            documentStore.store(d);
        
        // index
        if (index != null)
            index.index(d);
        
        // store RDF
        if (quadStore != null)
            quadStore.update(d);

        // after triggers
        for (Trigger t: getTriggers())
            if (add) t.afterAdd(this, d);
            else t.afterUpdate(this, d);
        
        return d.getIdentifier();
    }

    @Override
    public Document get(URI uri) throws WhelkException {
        if (documentStore != null)
            return documentStore.get(uri);
        else
            throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void delete(URI uri) throws WhelkException {
        for (Trigger t: getTriggers())
            t.beforeDelete(this, uri);

        if (documentStore != null) documentStore.delete(uri);
        if (index != null) index.delete(uri);
        if (quadStore != null) quadStore.delete(uri);
        
        for (Trigger t: getTriggers())
            t.afterDelete(this, uri);
    }

    @Override
    public Document document() {
        if (documentStore != null)
            return documentStore.document();
        else
            throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public InputStream sparql(String query) throws SparqlException {
        if (quadStore != null)
            return quadStore.sparql(query);
        else
            throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchResult find(Query query) throws WhelkException {
        if (index != null) return index.search(query);
        else throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Iterable<? extends Document> lookup(Key key) throws WhelkException {
        if (documentStore != null) return documentStore.lookup(key);
        else throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Iterable<? extends Key> browse(URI type, String start) throws WhelkException {
        if (documentStore != null) return documentStore.browse(type, start);
        else throw new UnsupportedOperationException("Not supported");
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
    public List<Plugin> getPlugins() {
        return plugins;
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

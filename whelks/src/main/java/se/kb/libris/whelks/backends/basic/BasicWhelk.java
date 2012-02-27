package se.kb.libris.whelks.backends.basic;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.backends.riak.RiakDocument;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.graph.QuadStore;
import se.kb.libris.whelks.graph.SparqlException;
import se.kb.libris.whelks.index.Index;
import se.kb.libris.whelks.index.Query;
import se.kb.libris.whelks.index.SearchResult;
import se.kb.libris.whelks.plugin.KeyGenerator;
import se.kb.libris.whelks.plugin.LinkFinder;
import se.kb.libris.whelks.plugin.Plugin;
import se.kb.libris.whelks.plugin.Trigger;
import se.kb.libris.whelks.storage.DocumentStore;

/**
 * @author marma
 */
public class BasicWhelk implements Whelk {
    DocumentStore documentStore = null;
    Index index = null;
    QuadStore quadStore = null;
    private List<Plugin> plugins = new LinkedList<Plugin>();
    
    public BasicWhelk(DocumentStore _documentStore, Index _index, QuadStore _quadStore) {
        documentStore = _documentStore;
        index = _index;
        quadStore = _quadStore;
    }
    
    /**
     * 
     */
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
        throw new UnsupportedOperationException("Not supported yet.");
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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream sparql(String query) throws SparqlException {
        if (quadStore != null)
            return quadStore.sparql(query);
        else
            throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SearchResult find(Query query) throws WhelkException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<? extends Document> lookup(Key key) throws WhelkException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<? extends Key> browse(URI type, String start) throws WhelkException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<KeyGenerator> getKeyGenerators() {
        List<KeyGenerator> ret = new LinkedList<KeyGenerator>();
        
        for (Plugin plugin: plugins)
            if (plugin instanceof KeyGenerator)
                ret.add((KeyGenerator)plugin);
        
        return ret;
    }

    private List<LinkFinder> getLinkFinders() {
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
}

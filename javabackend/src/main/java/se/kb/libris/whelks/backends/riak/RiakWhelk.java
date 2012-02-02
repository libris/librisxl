package se.kb.libris.whelks.backends.riak;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.Link;
import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.plugin.KeyGenerator;
import se.kb.libris.whelks.plugin.LinkFinder;
import se.kb.libris.whelks.plugin.Plugin;
import se.kb.libris.whelks.plugin.Trigger;

/**
 * @author marma
 */
public class RiakWhelk implements Whelk {
    private URL base;
    private String bucket = null;
    private List<Plugin> plugins = new LinkedList<Plugin>();
    
    static final int HTTP_CREATED = 201;
    
    public RiakWhelk(URL _base, String _bucket) {
        base = _base;
        bucket = _bucket;
    }
    
    @Override
    public URI add(Document d) throws WhelkException {
        // extract keys
        for (KeyGenerator kg: getKeyGenerators())
            kg.generateKeys(d);
        
        // find links
        for (LinkFinder lf: getLinkFinders())
            lf.findLinks(d);
        
        try {
            URL url = new URL(base, bucket + "/");
            
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", d.getContentType());
            
            buildRequest(conn, d);
            
            for (Trigger t: getTriggers())
                t.beforeAdd(d);

            int responseCode = conn.getResponseCode();
            
            if (responseCode == HTTP_CREATED) {
                String location = conn.getHeaderField("Location");
                
                if (location == null)
                    throw new WhelkException("Location is null");
                
                // @todo delete document
                if (!location.startsWith("/riak/" + bucket + "/"))
                    throw new WhelkException("Curious, document location is not in '/riak/" + bucket + "/, URI=" + location);
                
                ((RiakDocument)d).setIdentifier(new URI("riak:" + bucket + "/" + location.substring(7 + bucket.length())));
                
                for (Trigger t: getTriggers())
                    t.afterAdd(d);
            
                return d.getIdentifier();
            } else {
                throw new WhelkException("Could not create document (HTTP Status " + responseCode);
            }
        } catch (Exception e) {
            throw new WhelkException(e);
        }
    }

    @Override
    public URI update(Document d) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document get(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(URI uri) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document document() {
        return new RiakDocument();
    }
    @Override
    public Document document(URI identifier) {
        return new RiakDocument();
    }

    @Override
    public Iterable<? extends Document> find(String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<? extends Key> browse(URI type, String start) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Plugin> plugins() {
        return plugins;
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
    
    /*
    private <T> List<T> filter() {
        List<T> ret = new LinkedList<T>();
        
        for (Plugin plugin: plugins)
            if (plugin instanceof T)
                ret.add((T)plugin);
        
        return ret;
    }
     */
    
    private void buildRequest(HttpURLConnection conn, Document d) {
        
    }

    /*
     * @todo implement this to avoid iterating through whole list
     * of plugins each time
    private class PluginListWrapper extends LinkedList<Plugin> {
        private RiakWhelk whelk = null;
        
        private PluginListWrapper(RiakWhelk _whelk) {
            whelk = _whelk;
        }
        
        
    }
     */
}

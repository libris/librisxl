package se.kb.libris.whelks.backends.riak;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.storage.DocumentStore;

public class RiakStore implements DocumentStore {
    private URL base;
    private String bucket = null;
    
    static final int HTTP_CREATED = 201;
    
    public RiakStore(URL _base, String _bucket) {
        base = _base;
        bucket = _bucket;
    }
    
    @Override
    public URI store(Document d) throws WhelkException {
        try {
            URL url = new URL(base, bucket + "/");
            
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", d.getContentType());
            
            buildRequest(conn, d);
            
            int responseCode = conn.getResponseCode();
            
            if (responseCode == HTTP_CREATED) {
                String location = conn.getHeaderField("Location");
                
                if (location == null)
                    throw new WhelkException("Location is null");
                
                // @todo delete document
                if (!location.startsWith("/riak/" + bucket + "/"))
                    throw new WhelkException("Curious, document location is not in '/riak/" + bucket + "/, URI=" + location);
                
                ((RiakDocument)d).setIdentifier(new URI("riak:" + bucket + "/" + location.substring(7 + bucket.length())));
                
                return d.getIdentifier();
            } else {
                throw new WhelkException("Could not create document (HTTP Status " + responseCode + ")");
            }
        } catch (Exception e) {
            throw new WhelkException(e);
        }
    }

    @Override
    public Document get(URI uri) {
        try {
            return new RiakDocument(new URL(base, bucket + "/" + uri));
        } catch (MalformedURLException ex) {
            Logger.getLogger(RiakStore.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
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
/*
    @Override
    public Iterable<? extends Document> find(String query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
*/
    @Override
    public Iterable<? extends Document> lookup(Key key) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Iterable<? extends Key> browse(URI type, String start) {
        throw new UnsupportedOperationException("Not supported");
    }

    private void buildRequest(HttpURLConnection conn, Document d) {
        
    }

    
    
    /*
    @Override
    public List<Plugin> plugins() {
        return plugins;
    }

    private <T> List<T> filter() {
        List<T> ret = new LinkedList<T>();
        
        for (Plugin plugin: plugins)
            if (plugin instanceof T)
                ret.add((T)plugin);
        
        return ret;
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

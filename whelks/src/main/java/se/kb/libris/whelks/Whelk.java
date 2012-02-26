package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.plugin.Plugin;

public interface Whelk {
    public URI add(Document d) throws WhelkException;
    public URI update(Document d) throws WhelkException;
    public Document get(URI uri) throws WhelkException;
    public void delete(URI uri) throws WhelkException;
    public Document document();
    public Document document(URI identifier);
    public Iterable<? extends Document> find(String query) throws WhelkException;
    public Iterable<? extends Document> lookup(Key key) throws WhelkException;
    public Iterable<? extends Key> browse(URI type, String start) throws WhelkException;
    public List<Plugin> plugins();
}

package se.kb.libris.whelks.storage;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.exception.WhelkException;

/**
 * @author marma
 */
public interface DocumentStore {
    public URI store(Document d) throws WhelkException;
    public Document get(URI uri) throws WhelkException;
    public void delete(URI uri) throws WhelkException;
    public Iterable<? extends Document> lookup(Key key) throws WhelkException;
    public Iterable<? extends Key> browse(URI type, String start) throws WhelkException;
    public Document document();
    public Document document(URI identifier);
}

package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import se.kb.libris.whelks.exception.WhelkException;
import se.kb.libris.whelks.index.Query;
import se.kb.libris.whelks.index.SearchResult;

public interface Whelk {
    public URI store(Document d) throws WhelkException;
    public Document get(URI uri) throws WhelkException;
    public void delete(URI uri) throws WhelkException;
    public Document document();
    public InputStream sparql(String query);
    public SearchResult find(Query query) throws WhelkException;
    public Iterable<? extends Document> lookup(Key key) throws WhelkException;
    public Iterable<? extends Key> browse(URI type, String start) throws WhelkException;
}

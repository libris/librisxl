package se.kb.libris.whelks.index;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface Index {
    public SearchResult search(Query query) throws SearchException;
    public void index(Document doc) throws IndexException;
    public void delete(URI uri) throws IndexException;
}

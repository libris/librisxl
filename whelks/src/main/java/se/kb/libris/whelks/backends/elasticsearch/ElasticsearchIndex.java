package se.kb.libris.whelks.backends.elasticsearch;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.index.Index;
import se.kb.libris.whelks.index.IndexException;
import se.kb.libris.whelks.index.Query;
import se.kb.libris.whelks.index.SearchResult;

/**
 * @author marma
 */
public class ElasticsearchIndex implements Index {

    @Override
    public SearchResult search(Query query) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void index(Document doc) throws IndexException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(URI uri) throws IndexException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}

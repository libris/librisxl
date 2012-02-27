package se.kb.libris.whelks.graph;

import java.io.InputStream;
import java.net.URI;
import se.kb.libris.whelks.Document;

/**
 * @author marma
 */
public interface QuadStore {
    public void update(Document doc) throws QuadStoreException;
    public void delete(URI namedGraph) throws QuadStoreException;
    public InputStream sparql(String query) throws SparqlException;
}

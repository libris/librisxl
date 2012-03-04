package se.kb.libris.whelks.backends.jena;

import java.io.InputStream;
import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.graph.QuadStore;
import se.kb.libris.whelks.graph.QuadStoreException;
import se.kb.libris.whelks.graph.SparqlException;

/**
 * @author marma
 */
public class JenaQuadStore implements QuadStore {

    @Override
    public void update(Document doc) throws QuadStoreException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(URI namedGraph) throws QuadStoreException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream sparql(String query) throws SparqlException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}

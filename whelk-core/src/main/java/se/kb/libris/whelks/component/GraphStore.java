package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.InputStream;
import se.kb.libris.whelks.RDFDescription;
import se.kb.libris.whelks.result.SparqlResult;

public interface GraphStore extends Component {
    public InputStream sparql(String query);
    public void update(URI uri, RDFDescription d);
}

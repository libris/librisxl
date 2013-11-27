package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.RDFDescription;
import se.kb.libris.whelks.result.SparqlResult;

public interface GraphStore extends Component {
    public SparqlResult sparql(String query);
    public void update(URI uri, RDFDescription d);
}

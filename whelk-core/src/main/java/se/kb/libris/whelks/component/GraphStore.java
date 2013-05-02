package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.SparqlResult;

public interface GraphStore extends Component {
    public SparqlResult sparql(String query);
    public void update(URI uri, Document d);
}

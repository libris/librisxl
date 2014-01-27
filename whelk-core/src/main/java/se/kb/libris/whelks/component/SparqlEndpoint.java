package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.InputStream;

public interface SparqlEndpoint extends Component {
    public InputStream sparql(String query);
}

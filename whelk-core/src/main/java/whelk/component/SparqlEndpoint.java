package whelk.component;

import java.net.URI;
import java.io.InputStream;

public interface SparqlEndpoint extends Component {
    public InputStream sparql(String query);
}

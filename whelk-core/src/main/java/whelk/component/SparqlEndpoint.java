package whelk.component;

import java.io.InputStream;

public interface SparqlEndpoint extends Component {
    public InputStream sparql(String query);
}

package se.kb.libris.whelks.component;

import java.io.InputStream;
import java.net.URI;
import se.kb.libris.whelks.Document;

public interface QuadStore extends Component {
    public InputStream sparql(String query);
    public void update(URI uri, Document d);
    public void delete(URI graph);
}

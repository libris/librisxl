package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface QuadStore extends Component {
    public void update(URI uri, Document d);
    public void delete(URI graph);
}

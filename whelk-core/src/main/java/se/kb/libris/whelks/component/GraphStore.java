package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.RDFDescription;

public interface GraphStore extends Component {
    public void update(URI uri, RDFDescription d);
}

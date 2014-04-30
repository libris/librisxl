package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface GraphStore extends Component {
    void update(URI graphUri, Document doc);
}

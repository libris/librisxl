package whelk.component;

import java.net.URI;
import whelk.Document;

public interface GraphStore extends Component {
    void update(URI graphUri, Document doc);
}

package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface Archive extends Component {
    public Iterable<? extends Document> getHistory(URI uri);
}

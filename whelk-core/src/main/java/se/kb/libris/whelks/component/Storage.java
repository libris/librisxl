package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.OutputStream;
import se.kb.libris.whelks.Document;

public interface Storage extends Component {
    public void store(Document d, String whelkPrefix);
    public Document get(URI uri, String whelkPrefix);
    public Iterable<Document> getAll(String whelkPrefix);
}

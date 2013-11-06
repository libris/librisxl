package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.OutputStream;
import se.kb.libris.whelks.Document;

public interface Storage extends Component {
    public boolean store(Document d);
    public Document get(URI uri);
    public Iterable<Document> getAll(String dataset);
    public String getRequiredContentType();
}

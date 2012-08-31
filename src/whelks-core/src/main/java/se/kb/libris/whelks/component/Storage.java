package se.kb.libris.whelks.component;

import java.net.URI;
import java.io.OutputStream;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.LookupResult;

public interface Storage extends Component {
    public void store(Document d);
    public void store(Iterable<Document> d);
    public Document get(URI uri);
    public Iterable<Document> getAll();
    public void delete(URI uri);
    public LookupResult<? extends Document> lookup(Key key);
}

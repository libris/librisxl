package se.kb.libris.whelks.storage;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface Storage {
    public void store(Document d);
    public Document retrieve(URI u);
}

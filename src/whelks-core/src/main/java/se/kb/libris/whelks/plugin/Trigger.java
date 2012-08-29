package se.kb.libris.whelks.plugin;

import java.net.URI;
import se.kb.libris.whelks.Document;

public interface Trigger extends Plugin {
    public void beforeStore(Document d);
    public void afterStore(Document d);
    public void beforeGet(Document d);
    public void afterGet(Document d);
    public void beforeDelete(URI uri);
    public void afterDelete(URI uri);
}

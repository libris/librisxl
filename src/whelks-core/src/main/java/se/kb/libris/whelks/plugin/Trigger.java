package se.kb.libris.whelks.plugin;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;

public interface Trigger extends Plugin {
    public void beforeStore(Whelk whelk, Document d);
    public void afterStore(Whelk whelk, Document d);
    public void beforeGet(Whelk whelk, Document d);
    public void afterGet(Whelk whelk, Document d);
    public void beforeDelete(Whelk whelk, URI uri);
    public void afterDelete(Whelk whelk, URI uri);
}

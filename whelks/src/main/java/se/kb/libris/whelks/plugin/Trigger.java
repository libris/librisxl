package se.kb.libris.whelks.plugin;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;

public interface Trigger extends Plugin {
    public void beforeAdd(Whelk whelk, Document d);
    public void afterAdd(Whelk whelk, Document d);
    public void beforeUpdate(Whelk whelk, Document d);
    public void afterUpdate(Whelk whelk, Document d);
    public void beforeDelete(Whelk whelk, URI uri);
    public void afterDelete(Whelk whelk, URI uri);
}

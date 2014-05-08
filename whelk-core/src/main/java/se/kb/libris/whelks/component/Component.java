package se.kb.libris.whelks.component;

import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.plugin.*;
import se.kb.libris.whelks.*;

import java.net.URI;
import java.util.List;

public interface Component extends WhelkAware {

    public URI add(Document document);
    public void bulkAdd(List<Document> document, String contentType);
    public Document get(URI uri);
    public boolean handlesContent(String ctype);

    /**
     * Deletes an entry.
     * @param uri the URI of the entry to be deleted.
     * @param whelkId ID of the whelk calling the method. (May be null)
     */
    public void remove(URI uri);
}

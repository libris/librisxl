package se.kb.libris.whelks.component;

import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.plugin.Plugin;

import java.net.URI;

public interface Component extends Plugin {
    /**
     * Deletes an entry.
     * @param uri the URI of the entry to be deleted.
     * @param whelkId ID of the whelk calling the method. (May be null)
     */
    public void delete(URI uri, String whelkId);
}

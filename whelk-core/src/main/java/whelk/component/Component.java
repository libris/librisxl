package whelk.component;

import whelk.*;
import whelk.plugin.*;

import java.net.URI;
import java.util.List;

public interface Component extends WhelkAware {

    public Document get(URI uri);

    /**
     * Deletes an entry.
     * @param uri the URI of the entry to be deleted.
     * @param whelkId ID of the whelk calling the method. (May be null)
     */
    public void remove(URI uri);
    boolean handlesContent(String contentType);
}

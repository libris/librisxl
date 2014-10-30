package whelk.component;

import whelk.*;
import whelk.plugin.*;

import java.util.List;

public interface Component extends WhelkAware {

    /**
     * Deletes an entry.
     * @param identifier the identifier of the entry to be deleted.
     * @param whelkId ID of the whelk calling the method. (May be null)
     */
    public void remove(String identifier);
    boolean handlesContent(String contentType);
}

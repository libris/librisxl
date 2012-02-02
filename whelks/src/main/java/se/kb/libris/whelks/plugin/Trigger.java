package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;

/**
 * @author marma
 */
public interface Trigger extends Plugin {
    public void beforeAdd(Document d);
    public void afterAdd(Document d);
}

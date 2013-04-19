package se.kb.libris.whelks.component;

import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.plugin.Plugin;

import java.net.URI;

public interface Component extends Plugin {
    public void delete(URI uri, String whelkId);
}

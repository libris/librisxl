package se.kb.libris.whelks.component;

import se.kb.libris.whelks.Whelk;
import se.kb.libris.whelks.plugin.Plugin;

public interface Component extends Plugin {
    public void setWhelk(Whelk w);
}

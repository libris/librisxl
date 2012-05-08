package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Whelk;

public interface Plugin {
    public void setWhelk(Whelk w);
    public void enable();
    public void disable();
    public boolean isEnabled();
}

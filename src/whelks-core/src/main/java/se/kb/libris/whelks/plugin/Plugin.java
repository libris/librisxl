package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Whelk;

public interface Plugin {
    public String getId();
    public boolean isEnabled();
    public void enable();
    public void disable();
}

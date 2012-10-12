package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Whelk;

public interface Plugin extends Comparable<Plugin> {
    public String getId();
    public boolean isEnabled();
    public void enable();
    public void disable();
    public int getOrder();
}

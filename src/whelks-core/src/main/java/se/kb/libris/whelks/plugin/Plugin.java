package se.kb.libris.whelks.plugin;

public interface Plugin {
    public String getId();
    public String getName();
    public void enable();
    public void disable();
    public boolean isEnabled();
}

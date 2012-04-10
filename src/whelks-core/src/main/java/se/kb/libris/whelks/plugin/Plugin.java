package se.kb.libris.whelks.plugin;

public interface Plugin {
    public void enable();
    public void disable();
    public boolean isEnabled();
}

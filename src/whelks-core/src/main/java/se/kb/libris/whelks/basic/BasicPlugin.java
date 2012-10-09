package se.kb.libris.whelks.basic;

import se.kb.libris.whelks.plugin.Plugin;

public class BasicPlugin implements Plugin {
    private boolean enabled = true;

    public boolean isEnabled() { return enabled; }
    public void enable() { this.enabled = true; }
    public void disable() { this.enabled = false; }
    public String getId() { return "basicplugin"; }
}

package se.kb.libris.whelks.plugin;

import java.util.Map;

public interface Plugin {
    public String getId();
    public void init(String whelkId);
    public void addPlugin(Plugin p);
    // ecosystem
    public Map getGlobal();
}

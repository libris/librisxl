package whelk.plugin;

import java.util.Map;

public interface Plugin {
    public String getId();
    public void init();
    public void addPlugin(Plugin p);
    // ecosystem
    public Map getGlobal();
}

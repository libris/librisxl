package whelk.plugin;

import java.util.Map;

import whelk.Whelk;

public interface Plugin {
    public String getId();
    public void init();
    public void addPlugin(Plugin p);
    // ecosystem
    public Map getProps();
    public Whelk getWhelk();
}

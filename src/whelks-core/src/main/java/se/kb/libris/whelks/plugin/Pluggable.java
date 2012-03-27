package se.kb.libris.whelks.plugin;

import java.util.LinkedList;
import java.util.List;

/**
 * @author marma
 */
public interface Pluggable {
    public List<Plugin> getPlugins();
    public void addPlugin(Plugin plugin);
    public void removePlugin(String id);
}

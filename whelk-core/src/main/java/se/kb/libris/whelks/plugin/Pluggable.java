package se.kb.libris.whelks.plugin;

import java.util.LinkedList;
import java.util.List;

/**
 * @author marma
 */
public interface Pluggable {
    public Iterable<? extends Plugin> getPlugins();
    public void addPlugin(Plugin plugin);
    public void addPluginIfNotExists(Plugin plugin);
    public void removePlugin(String id);
}

package se.kb.libris.whelks.plugin;

import java.util.*;
import org.codehaus.jackson.map.*

public abstract class BasicPlugin implements Plugin {
    private boolean enabled = true;
    private String id = "basicPlugin";
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    private String initString = null
    private boolean initialized = false

    public final static mapper = new ObjectMapper()

    @Override
    public boolean isEnabled() { return enabled; }
    @Override
    public void setEnabled(boolean e) { this.enabled = e; }
    @Override
    public String getId() { return this.id; }
    public void setId(String i) { this.id = i; }
    @Override
    public void init(String initString) { }
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }
    public Map getGlobal() { global }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + id.hashCode();
        hash = hash * 15 + (enabled ? 0 : 1);
        return hash;
    }
}

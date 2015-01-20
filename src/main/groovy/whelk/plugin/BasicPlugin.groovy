package whelk.plugin;

import groovy.util.logging.Slf4j as Log

import java.util.*;
import java.lang.reflect.Method

import org.codehaus.jackson.map.*

import whelk.exception.*
import whelk.Whelk

@Log
public abstract class BasicPlugin implements WhelkAware {
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    Whelk whelk

    public final static mapper = new ObjectMapper()

    public final void init() {
        if (this.id == null) {
            throw new PluginConfigurationException("Plugin ${this.getClass().getName()} must have ID set before init()")
        }
        bootstrap()
    }
    void bootstrap() {
        log.debug("Bootstrapmethod not found on ${this.getClass().getName()}")
    }
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }
    public Map getGlobal() { global }
    public Plugin getPlugin(String pluginId) {
        plugins.find { it.id == pluginId }
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + (id?.hashCode() ?: 0)
        return hash;
    }
}

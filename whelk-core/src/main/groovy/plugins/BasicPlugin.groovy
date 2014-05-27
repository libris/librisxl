package se.kb.libris.whelks.plugin;

import groovy.util.logging.Slf4j as Log

import java.util.*;
import java.lang.reflect.Method

import org.codehaus.jackson.map.*

import se.kb.libris.whelks.exception.*

@Log
public abstract class BasicPlugin implements Plugin {
    private boolean enabled = true;
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map global

    public final static mapper = new ObjectMapper()

    @Override
    public boolean isEnabled() { return enabled; }
    @Override
    public void setEnabled(boolean e) { this.enabled = e; }
    @Override
    public final void init(String initString) {
        if (this.id == null) {
            throw new PluginConfigurationException("Plugin ${this.getClass().getName()} must have ID set before init()")
        }
        bootstrap(initString)
        /*
        Class c = this.getClass().getSuperclass()
        while (c && c.getName() != BasicPlugin.class.getName()) {
            try {
                Method m = c.getMethod("bootstrap", String.class)
                m.invoke(this, initString)
            } catch (NoSuchMethodException nsme) {
                log.debug("Bootstrapmethod not found on $c")
            }
            c = c.getSuperclass()
        }
        */
    }
    void bootstrap(String str) {
        log.debug("Bootstrapmethod not found on ${this.getClass().getName()}")
    }
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }
    public Map getGlobal() { global }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + (id?.hashCode() ?: 0)
        hash = hash * 15 + (enabled ? 0 : 1);
        return hash;
    }
}

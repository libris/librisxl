package whelk.camel.route

import groovy.util.logging.Slf4j as Log

import whelk.Whelk
import whelk.plugin.*
import whelk.exception.*

import org.apache.camel.*
import org.apache.camel.impl.*
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.model.dataformat.JsonLibrary

import org.apache.camel.component.http4.*

import org.codehaus.jackson.map.ObjectMapper

abstract class WhelkRouteBuilderPlugin extends RouteBuilder implements WhelkAware {

    // Properties copied from BasicPlugin
    String id = null
    private List<Plugin> plugins = new ArrayList<Plugin>();
    Map props

    Whelk whelk

    long batchTimeout = 5000
    int parallelProcesses = 20

    abstract String getMessageQueue()

    // Plugin methods
    @Override
    public void addPlugin(Plugin p) {
        plugins.add(p);
    }
    public List<Plugin> getPlugins() { plugins }

    public Plugin getPlugin(String pluginId) {
        return plugins.find { it.id == pluginId }
    }
    public final void init() {
        if (this.id == null) {
            throw new PluginConfigurationException("Plugin cannot have null id.")
        }
        bootstrap()
    }

    abstract void bootstrap()
}


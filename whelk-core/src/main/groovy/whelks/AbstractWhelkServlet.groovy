package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*
import java.util.regex.*

import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.*
import se.kb.libris.whelks.api.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

@Log
abstract class AbstractWhelkServlet extends HttpServlet {

    Map<Pattern, API> apis = new LinkedHashMap<Pattern, API>()
    final static ObjectMapper mapper = new ObjectMapper()
    // Global settings, set by whelk initializer
    Map global = [:]


    void handleRequest(HttpServletRequest request, HttpServletResponse response) {
        String path = request.pathInfo
        API api = null
        List pathVars = []

        log.debug("Path is $path")

        (api, pathVars) = getAPIForPath(path)
        if (api) {
            api.handle(request, response, pathVars)
        } else {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No API found for $path")
        }
    }

    def getAPIForPath(String path) {
        for (entry in apis.entrySet()) {
            log.trace("${entry.key} (${entry.key.getClass().getName()}) = ${entry.value}")
            Matcher matcher = entry.key.matcher(path)
            if (matcher.matches()) {
                int groupCount = matcher.groupCount()
                List pathVars = new ArrayList(groupCount)
                for (int i = 1; i <= groupCount; i++) {
                    pathVars.add(matcher.group(i))
                }
                return [entry.value, pathVars]
            }
        }
        return [null, []]
    }

    /**
     * Redirect request to handleRequest()-method
     */
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doPut(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }
    @Override
    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        handleRequest(request, response)
    }

    @Override
    void init() {
        try {
            def (whelkConfig, pluginConfig) = loadConfig()
            setConfig(whelkConfig, pluginConfig)
            log.info("[${this.id}] Whelk operational.")
        } catch (Exception e) {
            log.warn("Problems starting whelk ${this.id}.", e)
            throw e
        }
    }

    private def loadConfig() {
        Map whelkConfig
        Map pluginConfig
        if (System.getProperty("whelk.config.uri") && System.getProperty("plugin.config.uri")) {
            def wcu = System.getProperty("whelk.config.uri")
            def pcu = System.getProperty("plugin.config.uri")
            URI whelkconfig = new URI(wcu)
            URI pluginconfig = new URI(pcu)
            log.info("Initializing whelk using definitions in $wcu, plugins in $pcu")
            try {
                whelkConfig = mapper.readValue(new URI(wcu).toURL().newInputStream(), Map)
                pluginConfig = mapper.readValue(new URI(pcu).toURL().newInputStream(), Map)
            } catch (Exception e) {
                throw new PluginConfigurationException("Failed to read configuration: ${e.message}", e)
            }
        } else {
            throw new PluginConfigurationException("Could not find suitable config. Please set the 'whelk.config.uri' system property")
        }
        return [whelkConfig, pluginConfig]
    }

    private void setConfig(whelkConfig, pluginConfig) {
        def disabled = System.getProperty("disable.plugins", "").split(",")
        setId(whelkConfig["_id"])
        setDocBaseUri(whelkConfig["_docBaseUri"])
        this.global = whelkConfig["_properties"].asImmutable()
        whelkConfig["_plugins"].each { key, value ->
            log.trace("key: $key, value: $value")
            if (!(key =~ /^_.+$/)) {
                log.trace("Found a property to set for ${this.id}: $key = $value")
                this."$key" = value
            } else if (value instanceof List) {
                log.info("Adding plugins from group $key")
                for (p in value) {
                    if (!disabled.contains(p)) {
                        def plugin = getPlugin(pluginConfig, p, this.id)
                        log.info("Adding plugin ${plugin.id} to ${this.id}")
                        addPlugin(plugin)
                    } else {
                        log.info("Plugin \"${p}\" has been disabled because you said so.")
                    }
                }
            }
        }
        whelkConfig["_apis"].each { apiEntry ->
            apiEntry.each {
                log.debug("Found api: ${it.value}, should attach at ${it.key}")
                API api = getPlugin(pluginConfig, it.value, this.id)
                api.setWhelk(this)
                apis.put(Pattern.compile(it.key), api)
            }
        }
    }

    def translateParams(params, whelkname) {
        def plist = []
        if (params instanceof String) {
            for (param in params.split(",")) {
                param = param.trim()
                if (param == "_whelkname") {
                    plist << whelkname
                } else if (param.startsWith("_property:")) {
                    plist << global.get(param.substring(10))
                } else {
                    plist << param
                }
            }
        } else if (params instanceof Map) {
            params.each {
                if (it.value instanceof String && it.value.startsWith("_property")) {
                    params.put(it.key, global.get(it.value.substring(10)))
                }
            }
            plist << params
        } else {
            plist << params
        }
        return plist
    }

    def getPlugin(pluginConfig, plugname, whelkname) {
        def plugin
        pluginConfig.each { label, meta ->
            if (label == plugname) {
                if (meta._params) {
                    log.trace("Plugin $label has parameters.")
                    def params = translateParams(meta._params, whelkname)
                    log.trace("Plugin parameter: ${params}")
                    def pclasses = params.collect { it.class }
                    try {
                        def c = Class.forName(meta._class).getConstructor(pclasses as Class[])
                        log.trace("c: $c")
                        plugin = c.newInstance(params as Object[])
                        log.trace("plugin: $plugin")
                    } catch (NoSuchMethodException nsme) {
                        log.trace("Constructor not found the easy way. Trying to find assignable class.")
                        for (cnstr in Class.forName(meta._class).getConstructors()) {
                            log.trace("Found constructor for ${meta._class}: $cnstr")
                            log.trace("Parameter types: " + cnstr.getParameterTypes())
                            boolean match = true
                            int i = 0
                            for (pt in cnstr.getParameterTypes()) {
                                log.trace("Loop parameter type: $pt")
                                log.trace("Check against: " + params[i])
                                if (!pt.isAssignableFrom(params[i++].getClass())) {
                                    match = false
                                }
                            }
                            if (match) {
                                plugin = cnstr.newInstance(params as Object[])
                                break;
                            }
                        }
                    }
                } else {
                    log.trace("Plugin $label has no parameters.")
                    plugin = Class.forName(meta._class).newInstance()
                }
                assert plugin, "Failed to instantiate plugin: ${plugname} from class ${meta._class} with params ${meta._params}"
                if (meta._id) {
                    plugin.id = meta._id
                } else {
                    plugin.setId(label)
                }
                if (!meta._params) {
                    log.trace("$plugname has NO parameters.")
                } else {
                    log.trace("$plugname has parameters.")
                }
                if (meta._plugins) {
                    log.trace("Plugin has plugins.")
                    for (plug in meta._plugins) {
                        log.debug("Adding plugin $plug -> ${plugin.id}")
                        plugin.addPlugin(getPlugin(pluginConfig, plug, whelkname))
                    }
                } else {
                    log.trace("Plugin ${plugin.id} has no _plugin parameter ($meta)")
                }
            }
        }
        if (!plugin) {
            throw new WhelkRuntimeException("For $whelkname; unable to instantiate plugin with name $plugname.")
        }
        log.trace "Returning new or unique instance of $plugname"
        plugin.global = global
        return plugin
    }

    java.lang.reflect.Constructor findConstructor(Class c, Class p) {
        java.lang.reflect.Constructor constructor = null
        try {
            constructor = c.getConstructor(p)
            log.debug("Found constructor the classic way.")
        } catch (Exception e) {
            log.warn("Unable to get constructor for $p")
            constructor = null
        }
        if (!constructor) {
            for (cnstr in c.constructors) {
                if (cnstr.parameterTypes.length == 1 && cnstr.parameterTypes[0].isAssignableFrom(p)) {
                    log.debug("Found constructor for class $c with parameter $p : " + cnstr.paramterTypes()[0])
                    constructor = cnstr
                }
            }
        }
        return constructor 
    }

    // Methods declared in subclass
    abstract protected void setId(String id)
    abstract String getId()
    abstract void setDocBaseUri(String uri)
}

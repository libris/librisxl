package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class WhelkInitializer {
    def json
    def whelklist = []
    def plugins = [:]

    WhelkInitializer(InputStream wis, InputStream pis=null) {
        Object mapper = new ObjectMapper()
        log.info("Read config")
        json = mapper.readValue(wis, Map)
        if (pis) {
            log.info("Load separate plugin config")
            def extConf = mapper.readValue(pis, Map)
            def plugConf = json["_plugins"]
            if (plugConf) {
                plugConf.putAll(extConf)
            } else {
                plugConf = extConf
            }
            json["_plugins"] = plugConf
        }
    }

    def getWhelks() {
        def disabled = System.getProperty("disable.plugins", "").split(",")
        json._whelks.each { w ->
            w.each { wname, meta ->
                meta._class = meta._class ?: "se.kb.libris.whelks.StandardWhelk"
                def whelk = Class.forName(meta._class).getConstructor(String.class).newInstance(wname)
                whelk.global = json["_global_settings"].asImmutable()
                whelklist << whelk
                // Find setters for whelk.
                meta.each { key, value ->
                    if (!(key =~ /^_.+$/)) {
                        log.trace("Found a property to set for $wname: $key = $value")
                        whelk."$key" = value
                    } else if (value instanceof List) {
                        log.info("Adding plugins from group $key")
                        for (p in value) {
                            if (!disabled.contains(p)) {
                                def plugin = getPlugin(p, wname)
                                log.info("Adding plugin ${plugin.id} to ${whelk.id}")
                                whelk.addPlugin(plugin)
                            } else {
                                log.info("Plugin \"${p}\" has been disabled because you said so.")
                            }
                        }
                    }
                }
            }
        }
        return whelklist
    }

    def translateParams(params, whelkname) {
        def plist = []
        if (params instanceof String) {
            for (param in params.split(",")) {
                param = param.trim()
                if (param == "_whelkname") {
                    plist << whelkname
                } else if (param.startsWith("_whelk:")) {
                    def nameToFind = param.split(":")[1]
                    plist << whelklist.find { it.id == nameToFind }
                } else {
                    plist << param
                }
            }
        } else {
            plist << params
        }
        return plist
    }

    def getPlugin(plugname, whelkname) {
        if (plugins[plugname]) {
            log.trace "Recycling instance of $plugname"
            return plugins[plugname]
        }
        def plugin
        json._plugins.each { label, meta ->
            if (label == plugname) {
                if (meta._params) {
                    log.trace("Plugin $label has parameters.")
                    def params = translateParams(meta._params, whelkname)
                    log.debug("Plugin parameter: ${params}")
                    def pclasses = params.collect { it.class }
                    try {
                        def c = Class.forName(meta._class).getConstructor(pclasses as Class[])
                        log.debug("c: $c")
                        plugin = c.newInstance(params as Object[])
                        log.debug("plugin: $plugin")
                    } catch (NoSuchMethodException nsme) {
                        log.debug("Constructor not found the easy way. Trying to find assignable class.")
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
                /* Plugin order removed.
                if (meta._priority) {
                    log.debug("Setting priority ${meta._priority} for plugin $label")
                    plugin.order = meta._priority
                }
                */
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
                if (!meta._unique
                        && !(plugin instanceof WhelkAware)
                        && !meta._params) {
                    log.trace "$plugname not unique. Adding instance to map."
                    plugins[label] = plugin
                }
                if (meta._plugins) {
                    log.trace("Plugin has plugins.")
                    for (plug in meta._plugins) {
                        log.debug("Adding plugin $plug -> ${plugin.id}")
                        plugin.addPlugin(getPlugin(plug, whelkname))
                    }
                } else {
                    log.debug("Plugin ${plugin.id} has no _plugin parameter ($meta)")
                }
            }
        }
        if (!plugin) { 
            throw new WhelkRuntimeException("For $whelkname; unable to instantiate plugin with name $plugname.")
        }
        log.trace "Returning new or unique instance of $plugname"
        plugin.global = json["_global_settings"].asImmutable()
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
}

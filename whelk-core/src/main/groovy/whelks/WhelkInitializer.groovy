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

    WhelkInitializer(InputStream is) {
        Object mapper = new ObjectMapper()
        if (is) {
            json = mapper.readValue(is, Map)
        }
    }

    def getWhelks() {
        json._whelks.each { w ->
            w.each { wname, meta ->
                meta._class = meta._class ?: "se.kb.libris.whelks.StandardWhelk"
                def whelk = Class.forName(meta._class).getConstructor(String.class).newInstance(wname)
                // Find setters for whelk.
                meta.each { key, value ->
                    if (!(key =~ /^_.+$/)) {
                        log.trace("Found a property to set for $wname: $key = $value")
                        whelk."$key" = value
                    }
                }
                for (p in meta._plugins) {
                    def plugin = getPlugin(p, wname)
                    log.info("Adding plugin ${plugin.id} to ${whelk.id}")
                    whelk.addPlugin(plugin)
                }
                //log.debug("Initializing the whelk")
                //whelk.init()
                whelklist << whelk
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
                    plist << whelklist.find { it.id == params.split(":")[1] }
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
        json._plugins.each { p ->
            p.each { label, meta ->
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
                    if (meta._priority) {
                        log.debug("Setting priority ${meta._priority} for plugin $label")
                        plugin.order = meta._priority
                    }
                    if (meta._id) {
                        plugin.id = meta._id
                    } else {
                        plugin.setId(label)
                    }
                    if (!meta._unique 
                            && !(plugin instanceof WhelkAware) 
                            && (!meta._param || (meta._param instanceof String && !meta._param.split(",").find {it.startsWith("_")}))) {
                        log.trace "$plugname not unique. Adding instance to map."
                        plugins[label] = plugin
                    }
                }
            }
        }
        if (!plugin) { 
            throw new WhelkRuntimeException("For $whelkname; unable to instantiate plugin with name $plugname.")
        }
        log.trace "Returning new or unique instance of $plugname"
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

package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.plugin.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class WhelkInitializer {
    def json
    def whelklist = []

    WhelkInitializer(InputStream is) {
        Object mapper = new ObjectMapper()
        if (is) {
            json = mapper.readValue(is, Map)
        }
    }

    def getWhelks() {
        json._whelks.each { w ->
            w.each { wname, meta ->
                meta._class = meta._class ?: "se.kb.libris.whelks.basic.BasicWhelk"
                def whelk = Class.forName(meta._class).getConstructor(String.class).newInstance(wname)
                for (p in meta._plugins) {
                    whelk.addPlugin(getPlugin(p, wname))
                }
                whelklist << whelk
            }
        }
        return whelklist
    }

    def translateParams(params, whelkname) {
        if (params == "_whelkname") {
            return whelkname
        } 
        if (params instanceof String && params.startsWith("_whelk:")) {
            return whelklist.find { it.prefix == params.split(":")[1] }
        }
        return params 
    }

    def getPlugin(plugname, whelkname) {
        def plugins = [:]
        if (plugins[plugname]) {
            log.trace "Recycling instance of $plugname"
            return plugins[plugname]
        }
        def plugin
        json._plugins.each { p ->
            p.each { label, meta ->
                if (label == plugname) {
                    if (meta._params) {
                        def params = translateParams(meta._params, whelkname)
                        log.debug("Plugin parameter: ${params}")
                        try {
                            plugin = Class.forName(meta._class).getConstructor(params.getClass()).newInstance(params)
                        } catch (NoSuchMethodException nsme) {
                            log.debug("Constructor not found the easy way. Trying to find assignable class.")
                            for (cnstr in Class.forName(meta._class).getConstructors()) {
                                if (cnstr.getParameterTypes().length == 1 
                                        && cnstr.getParameterTypes()[0].isAssignableFrom(params.getClass())) {
                                    plugin = cnstr.newInstance(params)
                                }
                            }
                        }
                    } else {
                        plugin = Class.forName(meta._class).newInstance()
                    }
                    if (!plugin instanceof WhelkAware && !meta._param.startsWith("_") ) {
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

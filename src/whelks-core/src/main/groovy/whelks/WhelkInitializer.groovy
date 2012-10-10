package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.plugin.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class WhelkInitializer {
    def json
    def whelklist = []

    WhelkInitializer(InputStream is) {
        Object mapper = new ObjectMapper()
        json = mapper.readValue(is, Map)
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
                        meta._params = translateParams(meta._params, whelkname)
                        def paramclass = meta._params.getClass()
                        def constructors = 
                        plugin = Class.forName(meta._class).getDeclaredConstructor(meta._params.getClass()).newInstance(meta._params)
                    } else {
                        plugin = Class.forName(meta._class).newInstance()
                    }
                    if (!plugin instanceof WhelkAware && meta._param != "_whelkname") {
                        log.trace "$plugname not unique. Adding instance to map."
                        plugins[label] = plugin
                    }
                }
            }
        }
        log.trace "Returning new or unique instance of $plugname"
        return plugin
    }
}

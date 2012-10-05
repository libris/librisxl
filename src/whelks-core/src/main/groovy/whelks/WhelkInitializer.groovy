package se.kb.libris.whelks

import se.kb.libris.whelks.plugin.*

import org.codehaus.jackson.map.ObjectMapper

class WhelkInitializer {
    def json

    WhelkInitializer(InputStream is) {
        Object mapper = new ObjectMapper()
        json = mapper.readValue(is, Map)
    }

    def getWhelks() {
        def whelks = []
        json._whelks.each { w ->
            w.each { wname, meta ->
                meta._class = meta._class ?: "se.kb.libris.whelks.basic.BasicWhelk"  
                def whelk = Class.forName(meta._class).getConstructor(String.class).newInstance(wname)
                for (p in meta._plugins) {
                    whelk.addPlugin(getPlugin(p, wname))
                }
                whelks << whelk
            }
        }
        return whelks
    }

    def getPlugin(plugname, whelkname) {
        def plugins = [:]
        if (plugins[plugname]) {
            println "Recycling instance of $plugname"
            return plugins[plugname]
        }
        def plugin
        json._plugins.each { p ->
            p.each { label, meta ->
                if (label == plugname) {
                    if (meta._params) {
                        meta._params = meta._params == "_whelkname" ? whelkname : meta._params
                        plugin = Class.forName(meta._class).getConstructor(meta._params.getClass()).newInstance(meta._params)
                    } else {
                        plugin = Class.forName(meta._class).newInstance()
                    }
                    if (!plugin instanceof WhelkAware && meta._param != "_whelkname") {
                        println "$plugname not unique. Adding instance to map."
                        plugins[label] = plugin
                    }
                }
            }
        }
        println "Returning new or unique instance"
        return plugin
    }
}

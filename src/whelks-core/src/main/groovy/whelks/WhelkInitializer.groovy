package se.kb.libris.whelks

import org.codehaus.jackson.map.ObjectMapper

class WhelkInitializer {

    def json

    WhelkInitializer(InputStream is) {
        Object mapper = new ObjectMapper()
        json = mapper.readValue(is, Map)
    }

    def getWhelks() {
        def plugins = [:]
        json.plugins.each { p ->
            p.each { label, meta ->
                println "label $label, meta: $meta"
                if (meta._params) {
                    println "Params is of type: " + meta._params.getClass().getName()
                    plugins[label] = Class.forName(meta._class).getConstructor(meta._params.getClass()).newInstance(meta._params)
                } else {
                    plugins[label] = Class.forName(meta._class).newInstance()
                }
            }
        }
        println "plugins: $plugins"
    }
}

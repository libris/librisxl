package whelk.util

import java.text.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import org.codehaus.jackson.map.*

import whelk.*

class Tools {

    static ObjectMapper staticMapper = new ObjectMapper()

    /**
     * Detects the content-type of supplied data.
     * TODO: Implement properly.
     */
    static String contentType(byte[] data) {
        return "text/plain"
    }

    static String contentType(String data) {
        return contentType(data.getBytes('UTF-8'))
    }

    static def getDeepValue(Map map, String key) {
        def keylist = key.split(/\./)
        def lastkey = keylist[keylist.length-1]
        def result
        for (int i = 0; i < keylist.length; i++) {
            def k = keylist[i]
            while (map.containsKey(k)) {
                if (k == lastkey) {
                    result = map.get(k)
                    map = [:]
                } else {
                    if (map.get(k) instanceof Map) {
                        map = map.get(k)
                    } else {
                        result = []
                        for (item in map[k]) {
                            def dv = getDeepValue(item, keylist[i..-1].join("."))
                            if (dv) {
                                result << dv
                            }
                        }
                        map = [:]
                    }
                }
            }
        }
        if (!result && (lastkey == key)) {
            result = findNestedValueForKey(map, key)
        }
        return ((result && result instanceof List && result.size() == 1) ? result[0] : result)
    }

    static def findNestedValueForKey(Map map, String key) {
        def result
        map.each { k, v ->
            if (k == key) {
                result = v
            } else if (!result && v instanceof Map) {
                result = findNestedValueForKey(v, key)
            } else if (!result && v instanceof List) {
                v.each {
                    if (it instanceof Map) {
                        result = findNestedValueForKey(it, key)
                    }
                }
            }
        }
        return result
    }

    static Map insertAt(Map origmap, String path, Object newobject, boolean repeatable=false) {
        if (path) {
            if (path.endsWith("+")) {
                path = path[0..-2]
                repeatable = true
            }
            def m = origmap
            def keys = path.split(/\./)
            keys.eachWithIndex() { key, i ->
                if (i < keys.size()-1) {
                    if (!m.containsKey(key)) {
                        m.put(key, [:])
                        m = m.get(key)
                    } else if (m.get(key) instanceof Map) {
                        m = m.get(key)
                    } else {
                        def value = m.get(key)
                        def nm = [:]
                        m.put(key, [])
                        m[(key)] << value
                        m[(key)] << nm
                        m = nm
                    }
                }
            }
            def lastkey = keys[keys.size()-1]
            def lastvalue = m.get(lastkey)
            if (lastvalue != null) {
                if (!(lastvalue instanceof List)) {
                    m[lastkey] = [lastvalue]
                }
                m[lastkey] << newobject
            } else if (repeatable) {
                m[lastkey] = [newobject]
            } else {
                m[lastkey] = newobject
            }
        } else if (newobject instanceof Map) {
            origmap = origmap + newobject
        }
        return origmap
    }

    static Map mergeMap(Map origmap, Map newmap) {
        newmap.each { key, value -> 
            if (origmap.containsKey(key)) { // Update value for original map
                if (value instanceof Map && origmap.get(key) instanceof Map) {
                    origmap[key] = mergeMap(origmap.get(key), value)
                } else {
                    if (!(origmap.get(key) instanceof List)) {
                        origmap[key] = [origmap[key]]
                    }
                    origmap[key] << value
                }
            } else { // Add key to original map
                origmap[key] = value
            }
        }
        return origmap
    }

    static String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    static void printSpinner(String message, int currentCount) {
        def progressSpinner = ['/','-','\\','|']
        int state = currentCount % (progressSpinner.size()-1)
        print "${message}  ${progressSpinner[state]}                                                                 \r"
    }

    static Map getDataAsMap(Document doc) {
        return staticMapper.readValue(doc.getDataAsString(), Map)
    }

    static String getMapAsString(Map map) {
        return staticMapper.writeValueAsString(map)
    }
}

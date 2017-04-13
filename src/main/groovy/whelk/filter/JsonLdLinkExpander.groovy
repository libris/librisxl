package whelk.filter

import groovy.transform.Synchronized
import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.*
import whelk.component.*
import whelk.exception.*


@Log
class JsonLdLinkExpander {

    Map nodesToExpand = null
    private Map cachedDocuments = [:]
    PostgreSQLComponent storage

    static final ObjectMapper mapper = new ObjectMapper()

    JsonLdLinkExpander(PostgreSQLComponent s) {
        storage = s
        try {
            nodesToExpand = mapper.readValue(this.getClass().getClassLoader().getResourceAsStream("jsonLdNodesToExpand.json"), Map)
        } catch (Exception e) {
            log.error("Unable to find file \"jsonLdNodesToExpand.json\" in classpath.")
            throw e
        }
    }

    /**
     * Keys to preserve from linked document.
     */
    private static  List getRelevantKeys(String identifier) {
        if (identifier.startsWith("/def/enum/")) {
            return ["@id", "@type", "prefLabel"]
        }
        return null
    }

    boolean valid(Document doc) {
        if (doc && doc.contentType == "application/ld+json" && this.nodesToExpand.containsKey(doc.collection)) {
            return true
        }
        return false
    }

    @Synchronized
    void loadCachedDocuments() {
        log.debug("Caching def-documents.")
        for (doc in storage.loadAll("def")) {
            Map dataMap = (doc.data.containsKey("about") ? doc.data.get("about") : doc.data)
            dataMap.put("@id", doc.identifier)
            if (getRelevantKeys(doc.identifier)) {
                def newMap = [:]
                for (key in getRelevantKeys(doc.identifier)) {
                    if (dataMap.containsKey(key)) {
                        newMap.put(key, dataMap.get(key))
                    }
                }
                dataMap = newMap
            }
            log.debug("adding ${doc.identifier} ($dataMap) to cache")
            cachedDocuments.put(doc.identifier, dataMap)
        }
        log.debug("Cached ${cachedDocuments.size()} docs.")
    }

    Document filter(Document doc) {
        log.debug("Expanding ${doc.identifier}")
        def dataMap = doFilter(doc.data, doc.collection)
        return doc.withData(dataMap)
    }

    Map doFilter(Map dataMap, String collection) {
        if (!cachedDocuments) {
            loadCachedDocuments()
        }
        nodesToExpand[collection].each { key, instructions ->
            log.trace("key: $key, instructions: $instructions")
            def mapSegment = getNestedObject(key, dataMap)
            if (mapSegment instanceof List) {
                int i = 0
                for (map in mapSegment) {
                    mapSegment[i] = expandNode(map as Map, instructions)
                    i++
                }
            } else if (mapSegment instanceof Map) {
                log.trace("trying to replace mapsegment $mapSegment")
                def expandedNode = expandNode(mapSegment, instructions)
                setNestedObject(key, expandedNode, dataMap)
            } else if (mapSegment == null) {
                log.trace("Path $key not available.")
            } else {
                throw new WhelkRuntimeException("The path key \"$key\" does not point to a Map or a List. Please check configuration.")
            }
        }
        return dataMap
    }

    Map expandNode(Map node, Map instructions) {
        if (cachedDocuments.containsKey(node['@id'])) {
            return cachedDocuments.get(node['@id'])
        } else if (node['@id'] && !((String)node['@id']).startsWith("/def/")) {
            Location loc = storage.locate(node['@id'] as String)
            if (!loc) {
                return node
            } else if (loc.getDocument()) {
                return loc.document.data
            } else {
                return storage.load(loc.uri.toString())?.data
            }
        }
        return node
    }

    static def getNestedObject(String key, Map map) {
        Map m = map
        for (k in key.split(/\./)) {
            if (m && m.containsKey(k)) {
                m = m[k]
            } else {
                return null
            }
        }
        return m
    }

    static void setNestedObject(key, node, dataMap) {
        def m = dataMap
        def elems = key.split(/\./)
        for (int i=0; i < elems.size(); i++) {
            if (i+1 == elems.size()) {
                m.put(elems[i], node)
            } else {
                m = m.get(elems[i])
            }
        }
    }
}



package whelk.util

public class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String ID_KEY = "@id"

    static Map flatten(Map framedJsonLd) {
    }

    static Map frame(String mainId, Map flatJsonLd) {
        if (isFramed(flatJsonLd)) {
            return flatJsonLd
        }

        def idMap = getIdMap(flatJsonLd)
        def framedMap = idMap.get(mainId)

        framedMap = embed(framedMap, idMap)

        return framedMap
    }

    static boolean isFlat(Map jsonLd) {
        if (jsonLd.size() == 1 && jsonLd.containsKey(GRAPH_KEY)) {
            return true
        }
        return false
    }

    static boolean isFramed(Map jsonLd) {
        return !isFlat(jsonLd)
    }

    private static embed(Map framedMap, Map idMap) {
        framedMap.each { key, value ->
            if (value instanceof Map && value.containsKey(ID_KEY) && idMap.containsKey(value.get(ID_KEY))) {
                framedMap.put(key, embed(idMap.get(value.get(ID_KEY)), idMap))
            }
            if (value instanceof List) {
                def newList = []
                for (l in value) {
                    if (l instanceof Map && l.containsKey(ID_KEY) && idMap.containsKey(l.get(ID_KEY))) {
                        newList.add(embed(idMap.get(l.get(ID_KEY)), idMap))
                    } else {
                        newList.add(l)
                    }
                }
                framedMap.put(key, newList)
            }
        }
    }

    private static Map getIdMap(Map flatJsonLd) {
        Map idMap = [:]
        for (item in flatJsonLd.get(GRAPH_KEY)) {
            if (item.containsKey(ID_KEY)) {
                idMap.put(item.get(ID_KEY), item)
            }
        }
        return idMap
    }
}

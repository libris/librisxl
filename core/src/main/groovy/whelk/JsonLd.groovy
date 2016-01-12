package whelk

import whelk.exception.FramingException

public class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String ID_KEY = "@id"
    static final String DESCRIPTIONS_KEY = "descriptions"
    static final URI SLASH_URI = new URI("/")

    // TODO: This flatten-method does not create description-based flat json (i.e. with entry, items and quoted)
    static Map flatten(Map framedJsonLd) {
        if (isFlat(framedJsonLd) || !framedJsonLd.containsKey(ID_KEY)) {
            return framedJsonLd
        }

        def flatList = []

        storeFlattened(framedJsonLd, flatList)

        return [(GRAPH_KEY): flatList.reverse()]
    }

    static Map flattenWithDescriptions(Map framedJsonLd) {
        if (isFlat(framedJsonLd)) {
            return framedJsonLd
        }
        def descriptionsFlatMap = ["descriptions": [:]]
        flatten(framedJsonLd).eachWithIndex { i, item ->
            if (i == 0) {
                descriptionsFlatMap['descriptions']['entry'] = item
            }
        }
    }


    private static storeFlattened(current, result) {
        if (current instanceof Map) {
            def flattened = makeFlat(current, result)
            if (flattened.containsKey(ID_KEY) && flattened.size() > 1) {
                result.add(flattened)
            }
            def itemid = current.get(ID_KEY)
            return (itemid ? [(ID_KEY): itemid] : current)
        }
        return current
    }

    private static makeFlat(obj, result) {
        def updated = [:]
        obj.each { key, value ->
            if (value instanceof List) {
                def newvaluelist = []
                for (o in value) {
                    newvaluelist.add(storeFlattened(o, result))
                }
                value = newvaluelist
            } else {
                value = storeFlattened(value, result)
            }
            updated[(key)] = value
        }
        return updated
    }


    public static Map frame(String mainId, Map flatJsonLd) {
        mainId = Document.BASE_URI.resolve(mainId)
        if (isFramed(flatJsonLd)) {
            return flatJsonLd
        }
        def idMap = getIdMap(flatJsonLd)
        def mainItemMap = idMap.get(mainId)
        if (!mainItemMap) {
            // Try to find an identifier to frame around
            String foundIdentifier = findIdentifier(flatJsonLd)
            if (foundIdentifier) {
                mainItemMap = idMap.get(SLASH_URI.resolve(foundIdentifier).toString())
            }
        }
        Map framedMap
        try {
            framedMap = embed(mainId, mainItemMap, idMap, new HashSet<String>())
            if (!framedMap) {
                throw new FramingException("Failed to frame JSONLD ($flatJsonLd)")
            }
        } catch (StackOverflowError sofe) {
            throw new FramingException("Unable to frame JSONLD ($flatJsonLd). Recursive loop?)", sofe)
        }

        return framedMap
    }

    private static Map embed(String mainId, Map mainItemMap, Map idMap, Set embedChain) {
        embedChain.add(mainId)
        mainItemMap.each { key, value ->
            mainItemMap.put(key, toEmbedded(value, idMap, embedChain))
        }
        return mainItemMap
    }

    private static Object toEmbedded(Object o, Map idMap, Set embedChain) {
        if (o instanceof List) {
            def newList = []
            o.each {
                newList.add(toEmbedded(it, idMap, embedChain))
            }
            return newList
        }
        if (o instanceof Map) {
            def oId = o.get(ID_KEY)
            if (oId && !embedChain.contains(oId)) {
                def obj = idMap.get(oId)
                if (obj) {
                    return embed(oId, obj, idMap, embedChain)
                }
            }
        }
        return o
    }

    static URI findRecordURI(Map jsonLd) {
        String foundIdentifier = findIdentifier(jsonLd)
        if (foundIdentifier) {
            return Document.BASE_URI.resolve(foundIdentifier)
        }
        return null
    }

    static String findIdentifier(Map jsonLd) {
        String foundIdentifier = null
        if (!jsonLd) {
            return null
        }
        if (isFlat(jsonLd)) {
            if (jsonLd.containsKey(GRAPH_KEY)) {
                foundIdentifier = jsonLd.get(GRAPH_KEY).first().get(ID_KEY)
            }
            if (jsonLd.containsKey(DESCRIPTIONS_KEY)) {
                foundIdentifier = jsonLd.get(DESCRIPTIONS_KEY).get("entry").get(ID_KEY)
            }
        }
        if (isFramed(jsonLd)) {
            foundIdentifier = jsonLd.get(ID_KEY)
        }
        if (foundIdentifier) {
            if (foundIdentifier.startsWith("/") || foundIdentifier.startsWith(Document.BASE_URI.toString())) {
                // Assumes only identifier in uri path
                return Document.BASE_URI.resolve(foundIdentifier).getPath().substring(1)
            }
            return foundIdentifier
        }
        return null
    }



    static boolean isFlat(Map jsonLd) {
        if (jsonLd.size() == 1 && (jsonLd.containsKey(GRAPH_KEY) || jsonLd.containsKey(DESCRIPTIONS_KEY))) {
            return true
        }
        return false
    }

    static boolean isFramed(Map jsonLd) {
        if (jsonLd.size() > 1 && !jsonLd.containsKey(GRAPH_KEY) && !jsonLd.containsKey(DESCRIPTIONS_KEY)) {
            return true
        }
        return false
    }

    private static Map getIdMap(Map flatJsonLd) {
        Map idMap = [:]
        if (flatJsonLd.containsKey(GRAPH_KEY)) {
            for (item in flatJsonLd.get(GRAPH_KEY)) {
                if (item.containsKey(ID_KEY)) {
                    idMap.put(item.get(ID_KEY), item)
                }
            }
        } else if (flatJsonLd.containsKey(DESCRIPTIONS_KEY)) {
            idMap.put(flatJsonLd.get(DESCRIPTIONS_KEY).get("entry").get(ID_KEY), flatJsonLd.get(DESCRIPTIONS_KEY).get("entry"))
            for (item in flatJsonLd.get(DESCRIPTIONS_KEY).get("items")) {
                if (item.containsKey(ID_KEY)) {
                    idMap.put(item.get(ID_KEY), item)
                }
            }
            for (item in flatJsonLd.get(DESCRIPTIONS_KEY).get("quoted")) {
                if (item.get(GRAPH_KEY).containsKey(ID_KEY)) {
                    idMap.put(item.get(GRAPH_KEY).get(ID_KEY), item.get(GRAPH_KEY))
                }
            }

        }
        return idMap
    }
}

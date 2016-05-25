package whelk

import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import whelk.exception.FramingException
import whelk.exception.ModelValidationException

public class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String ID_KEY = "@id"
    static final String DESCRIPTIONS_KEY = "descriptions"
    static final URI SLASH_URI = new URI("/")
    static final ObjectMapper mapper = new ObjectMapper()

    static final MARCFRAMEMAP = mapper.readValue(JsonLd.getClassLoader().getResourceAsStream("ext/marcframe.json"), Map)

    private static Logger log = LoggerFactory.getLogger(JsonLd.class)

    /**
     * This flatten-method does not create description-based flat json (i.e. with entry, items and quoted)
     */
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
        if (isFramed(flatJsonLd)) {
            return flatJsonLd
        }
        Map idMap = getIdMap(flatJsonLd)
        if (mainId) {
            mainId = Document.BASE_URI.resolve(mainId)
        }
        Map mainItemMap = (mainId ? idMap.get(mainId) : null)
        if (!mainItemMap) {
            log.debug("No main item map found for $mainId, trying to find an identifier")
            // Try to find an identifier to frame around
            String foundIdentifier = findIdentifier(flatJsonLd)
            log.debug("Result of findIdentifier: $foundIdentifier")
            if (foundIdentifier) {
                mainItemMap = idMap.get(Document.BASE_URI.resolve(foundIdentifier).toString())
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

        Set referencedBNodes = new HashSet()
        getReferencedBNodes(framedMap, referencedBNodes)

        cleanUnreferencedBNodeIDs(framedMap, referencedBNodes)

        return framedMap
    }

    /**
     * Fills the referencedBNodes set with all "_:*" ids that are referenced anywhere in the structure/document
     * (and thus cannot be safely removed)
     */
    public static void getReferencedBNodes(Map map, Set referencedBNodes)
    {
        // A jsonld reference is denoted as a json object containing exactly one member, with the key "@id".
        if (map.size() == 1)
        {
            String key = map.keySet().getAt(0)
            if (key.equals("@id"))
            {
                String id = map.get(key)
                if (id.startsWith("_:"))
                    referencedBNodes.add(id)
            }
        }

        for (Object keyObj : map.keySet())
        {
            Object subobject = map.get(keyObj);

            if ( subobject instanceof Map )
                getReferencedBNodes( (Map) subobject, referencedBNodes );
            else if ( subobject instanceof List )
                getReferencedBNodes( (List) subobject, referencedBNodes );
        }
    }

    public static void getReferencedBNodes(List list, Set referencedBNodes)
    {
        for (Object item : list)
        {
            if ( item instanceof Map )
                getReferencedBNodes( (Map) item, referencedBNodes );
        }
    }

    public static void cleanUnreferencedBNodeIDs(Map map, Set referencedBNodes)
    {
        if (map.size() > 1)
        {
            if (map.containsKey("@id"))
            {
                String id = map.get("@id")

                if (id.startsWith("_:") && !referencedBNodes.contains(id))
                {
                    map.remove("@id")
                }
            }
        }

        for (Object keyObj : map.keySet())
        {
            Object subobject = map.get(keyObj);

            if ( subobject instanceof Map )
                cleanUnreferencedBNodeIDs( (Map) subobject, referencedBNodes );
            else if ( subobject instanceof List )
                cleanUnreferencedBNodeIDs( (List) subobject, referencedBNodes );
        }
    }

    public static void cleanUnreferencedBNodeIDs(List list, Set referencedBNodes)
    {
        for (Object item : list)
        {
            if ( item instanceof Map )
                cleanUnreferencedBNodeIDs( (Map) item, referencedBNodes );
        }
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
            def obj = null
            def oId = o.get(ID_KEY)
            if (!oId) {
                obj = o
            } else if (!embedChain.contains(oId)) {
                obj = idMap.get(oId)
            }
            if (obj) {
                return embed(oId, obj, idMap, embedChain)
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
            log.trace("Received json is flat")
            if (jsonLd.containsKey(GRAPH_KEY)) {
                foundIdentifier = jsonLd.get(GRAPH_KEY).first().get(ID_KEY)
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
        if ((jsonLd.containsKey(GRAPH_KEY) && jsonLd.get(GRAPH_KEY) instanceof List || jsonLd.containsKey(DESCRIPTIONS_KEY))) {
        //if (jsonLd.size() == 1 && (jsonLd.containsKey(GRAPH_KEY) || jsonLd.containsKey(DESCRIPTIONS_KEY))) {
            return true
        }
        return false
    }

    static boolean isFramed(Map jsonLd) {
        if (jsonLd && !jsonLd.containsKey(GRAPH_KEY) && !jsonLd.containsKey(DESCRIPTIONS_KEY)) {
            return true
        }
        return false
    }

    private static Map getIdMap(Map flatJsonLd) {
        Map idMap = [:]
        if (flatJsonLd.containsKey(GRAPH_KEY)) {
            for (item in flatJsonLd.get(GRAPH_KEY)) {
                if (item.containsKey(GRAPH_KEY)) {
                    item = item.get(GRAPH_KEY)
                }
                if (item.containsKey(ID_KEY)) {
                    def id = item.get(ID_KEY)
                    if (idMap.containsKey(id)) {
                        Map existing = idMap.get(id)
                        idMap.put(id, existing + item)
                    } else {
                        idMap.put(id, item)
                    }
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

    static boolean validateItemModel(Document doc) {
        if (!doc || !doc.data) {
            throw new ModelValidationException("Document has no data to validate.")
        }
        Map docData = frame(doc.id, doc.data)
        String thingLink = MARCFRAMEMAP.get(doc.collection)?.get("thingLink")
        String collectionType = MARCFRAMEMAP[doc.collection]['000']['_spec'][0]["result"][thingLink]["@type"]

        Map about = (docData.containsKey(thingLink) ? docData.get(thingLink) : docData )
        String type = about.get("@type")
        if (type != collectionType) {
            throw new ModelValidationException("Document has mismatching @type ($type) for ${collectionType}.")
        }
        if (doc.collection == "hold") {
            if (!about.containsKey("heldBy") || !about.heldBy.containsKey("notation")) {
                throw new ModelValidationException("Item is missing heldBy declaration.")
            }
            // TODO: Remove holdingFor option when data is coherent
            Map itemOf = (about.containsKey("itemOf") ? about.get("itemOf") : about.get("holdingFor"))
            if (!itemOf?.containsKey("@id")) {
                throw new ModelValidationException("Item is missing itemOf declaration.")
            }
        }
        return true
    }
}

package whelk

import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.Datafield
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.exception.FramingException
import whelk.exception.ModelValidationException

import se.kb.libris.util.marc.io.MarcXmlRecordReader
import se.kb.libris.util.marc.MarcRecord
import whelk.util.PropertyLoader

public class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String CONTEXT_KEY = "@context"
    static final String ID_KEY = "@id"
    static final String TYPE_KEY = "@type"
    static final String REVERSE_KEY = "@reverse"
    static final String THING_KEY = "mainEntity"
    static final String RECORD_KEY = "meta"
    static final String CREATED_KEY = "created"
    static final String MODIFIED_KEY = "modified"
    static final String DELETED_KEY = "deleted"
    static final String COLLECTION_KEY = "collection"
    static final String CONTENT_TYPE_KEY = "contentType"
    static final String CHECKSUM_KEY = "checksum"
    static final String NON_JSON_CONTENT_KEY = "content"
    static final String ALTERNATE_ID_KEY = "identifiers"
    static final String JSONLD_ALT_ID_KEY = "sameAs"
    static final String CONTROL_NUMBER_KEY = "controlNumber"
    static final String ABOUT_KEY = "mainEntity"
    static final String APIX_FAILURE_KEY = "apixExportFailedAt"
    static final String ENCODING_LEVEL_KEY = "marc:encLevel"
    static final String HOLDING_FOR_KEY = "holdingFor"

    static final ObjectMapper mapper = new ObjectMapper()
    static final JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter()

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

    public static List getExternalReferences(Map jsonLd){
        Set allReferences = getAllReferences(jsonLd)
        Set localObjects = getLocalObjects(jsonLd)
        List externalRefs = allReferences.minus(localObjects) as List
        // NOTE: this is necessary because some documents contain references to
        // bnodes that don't exist (in that document).
        return filterOutDanglingBnodes(externalRefs)
    }

    static List expandLinks(List refs, Map context) {
        List result = []
        refs.each { ref ->
            def match
            if (ref =~ $/^https?:///$) {
                result << ref
            } else if ((match = ref =~ /^([a-z0-9]+):(.*)$/)) {
                def resolved = context[match[0][1]]
                if (resolved) {
                    URI base = new URI(resolved)
                    result << base.resolve(match[0][2]).toString()
                }
            } else {
                result << ref
            }
        }

        return result
    }

    private static Set getLocalObjects(Map jsonLd) {
        List result = []
        if (jsonLd.get(GRAPH_KEY)) {
            for (item in jsonLd.get(GRAPH_KEY)) {
                if (item.containsKey(GRAPH_KEY)) {
                    result.addAll(getLocalObjects(item))
                }
                if (item.containsKey(ID_KEY)) {
                    def id = item.get(ID_KEY)
                    if (!result.contains(id)) {
                        result << id
                    }
                }
                if (item.containsKey(JSONLD_ALT_ID_KEY)) {
                    item.get(JSONLD_ALT_ID_KEY).each {
                        if (!it.containsKey(ID_KEY)) {
                            return
                        }

                        def id = it.get(ID_KEY)
                        if (!result.contains(id)) {
                            result << id
                        }
                    }
                }
            }
        }
        return result
    }

    private static List filterOutDanglingBnodes(List refs) {
        return refs.findAll {
            !it.startsWith('_:')
        }
    }

    public static Set getAllReferences(Map jsonLd) {
        List items
        if (jsonLd.containsKey(GRAPH_KEY)) {
            items = jsonLd.get(GRAPH_KEY)
        } else {
            throw new FramingException("Missing '@graph' key in input")
        }
        return getAllReferencesFromList(items).flatten()
    }

    private static Set getRefs(Object o) {
        if(o instanceof Map) {
            return getAllReferencesFromMap(o)
        } else if (o instanceof List){
            return getAllReferencesFromList(o)
        } else {
            return []
        }
    }

    private static Set getAllReferencesFromMap(Map item) {
        Set refs = []

        if (isReference(item)) {
            refs.add(item[ID_KEY])
            return refs
        } else {
            item.each { key, value ->
                refs << getRefs(value)
            }
        }

        return refs
    }

    private static boolean isReference(Map map) {
        if(map.get(ID_KEY) && map.size() == 1) {
            return true
        } else {
            return false
        }
    }

    private static Set getAllReferencesFromList(List items) {
        Set result = []
        items.each { item ->
            result << getRefs(item)
        }
        return result
    }

    public static Map embellish(Map jsonLd, Map additionalObjects, Map displayData) {
        if (!jsonLd.get(GRAPH_KEY)) {
            return jsonLd
        }

        List graphItems = jsonLd.get(GRAPH_KEY)

        additionalObjects.each { id, object ->
            Map chip = toChip(object, displayData)
            if (chip.containsKey('@graph')) {
                if (!chip.containsKey('@id')) {
                    chip['@id'] = id
                }
                graphItems << chip
            } else {
                graphItems << ['@graph': chip,
                               '@id': id]
            }
        }
        jsonLd[GRAPH_KEY] = graphItems

        return jsonLd
    }


    /**
     * Convert a list of posts to cards.
     *
     */
    public static List toCards(List things, Map displayData) {
        return things.collect { toCard(it, displayData) }
    }

    /**
     * Convert a post to card.
     *
     */
    public static Map toCard(Map thing, Map displayData) {
        Map lensGroups = displayData.get("lensGroups")
        Map cardLensGroup = lensGroups.get("cards")
        Map result = [:]

        Map card = removeProperties(thing, cardLensGroup)
        card.each {key, value ->
            result[key] = toChip(value, displayData)
        }
        return result
    }

    /**
     * Convert a list of posts to chips.
     *
     */
    public static List toChips(List things, Map displayData) {
        return things.collect { toChip(it, displayData) }
    }

    /**
     * Convert a post to chip.
     *
     */
    public static Object toChip(Object object, Map displayData) {
        Map lensGroups = displayData.get("lensGroups")
        Map chipLensGroup = lensGroups.get("chips")
        Map itemsToKeep = [:]
        Map result = [:]

        if (object instanceof List){
            return toChips(object, displayData)
        } else if ((object instanceof Map)) {
            itemsToKeep = removeProperties(object, chipLensGroup)
            itemsToKeep.each {key, value ->
                result[key] = toChip(value, displayData)
            }
            return result
        } else {
            return object
        }
    }

    private static Map removeProperties(Map jsonMap, Map lensGroups) {
        Map itemsToKeep = [:]
        Map types = lensGroups.get("lenses")
        String type = jsonMap.get("@type")

        if (!type) {
            return jsonMap
        }

        Map showPropertiesField = types.get(type)

        if (showPropertiesField) {
            List propertiesToKeep = showPropertiesField.get("showProperties")

            jsonMap.each {key, value ->
                if (shouldKeep(key, propertiesToKeep)) {
                    itemsToKeep[key] = value
                }
            }
            return itemsToKeep
        } else {
            return jsonMap
        }
    }

    private static boolean shouldKeep(String key, List propertiesToKeep) {
        return (key in propertiesToKeep || key.startsWith("@"))
    }


    public static Map frame(String mainId, Map flatJsonLd) {
        return frame(mainId, null, flatJsonLd)
    }

    public static Map frame(String mainId, String thingLink, Map flatJsonLd, boolean mutate = false) {
        if (isFramed(flatJsonLd)) {
            return flatJsonLd
        }

        Map flatCopy = mutate ? flatJsonLd : (Map) Document.deepCopy(flatJsonLd)

        if (mainId) {
            mainId = Document.BASE_URI.resolve(mainId)
        }

        def idMap = getIdMap(flatCopy)

        def mainItem = idMap[mainId]
        if (mainItem) {
            if (thingLink) {
                def thingRef = mainItem[thingLink]
                if (thingRef) {
                    def thingId = thingRef[ID_KEY]
                    def thing = idMap[thingId]
                    thing[RECORD_KEY] = [(ID_KEY): mainId]
                    mainId = thingId
                    idMap[mainId] = thingRef
                    mainItem = thing
                    log.debug("Using think-link. Framing around ${mainId}")
                }
            }
        } else {
            log.debug("No main item map found for $mainId, trying to find an identifier")
            // Try to find an identifier to frame around
            String foundIdentifier = Document.BASE_URI.resolve(findIdentifier(flatCopy))

            log.debug("Result of findIdentifier: $foundIdentifier")
            if (foundIdentifier) {
                mainItem = idMap.get(foundIdentifier)
            }
        }
        Map framedMap
        try {
            framedMap = embed(mainId, mainItem, idMap, new HashSet<String>())
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
    public static void getReferencedBNodes(Map map, Set referencedBNodes) {
        // A jsonld reference is denoted as a json object containing exactly one member, with the key "@id".
        if (map.size() == 1) {
            String key = map.keySet().getAt(0)
            if (key.equals("@id")) {
                String id = map.get(key)
                if (id.startsWith("_:"))
                    referencedBNodes.add(id)
            }
        }

        for (Object keyObj : map.keySet()) {
            Object subobject = map.get(keyObj)

            if (subobject instanceof Map)
                getReferencedBNodes((Map) subobject, referencedBNodes)
            else if (subobject instanceof List)
                getReferencedBNodes((List) subobject, referencedBNodes)
        }
    }

    public static void getReferencedBNodes(List list, Set referencedBNodes) {
        for (Object item : list) {
            if (item instanceof Map)
                getReferencedBNodes((Map) item, referencedBNodes)
        }
    }

    public static void cleanUnreferencedBNodeIDs(Map map, Set referencedBNodes) {
        if (map.size() > 1) {
            if (map.containsKey("@id")) {
                String id = map.get("@id")

                if (id.startsWith("_:") && !referencedBNodes.contains(id)) {
                    map.remove("@id")
                }
            }
        }

        for (Object keyObj : map.keySet()) {
            Object subobject = map.get(keyObj)

            if (subobject instanceof Map)
                cleanUnreferencedBNodeIDs((Map) subobject, referencedBNodes)
            else if (subobject instanceof List)
                cleanUnreferencedBNodeIDs((List) subobject, referencedBNodes)
        }
    }

    public static void cleanUnreferencedBNodeIDs(List list, Set referencedBNodes) {
        for (Object item : list) {
            if (item instanceof Map)
                cleanUnreferencedBNodeIDs((Map) item, referencedBNodes)
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

    static String findFullIdentifier(Map jsonLd) {
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

        return foundIdentifier
    }

    static String findIdentifier(Map jsonLd) {
        String foundIdentifier = findFullIdentifier(jsonLd)

        if (foundIdentifier) {
            if (foundIdentifier.startsWith("/") || foundIdentifier.startsWith(Document.BASE_URI.toString())) {
                // Assumes only identifier in uri path
                return Document.BASE_URI.resolve(foundIdentifier).getPath().substring(1)
            }
            return foundIdentifier
        } else {
            return null
        }
    }



    static boolean isFlat(Map jsonLd) {
        if ((jsonLd.containsKey(GRAPH_KEY) && jsonLd.get(GRAPH_KEY) instanceof List)) {
            return true
        }
        return false
    }

    static boolean isFramed(Map jsonLd) {
        if (jsonLd && !jsonLd.containsKey(GRAPH_KEY)) {
            return true
        }
        return false
    }

    /*
     * Traverse the JSON doc and grab all @id and their respective objects
     *
     * This is then useful for framing, since we can easily find the object
     * we'll replace the reference with.
     *
     */
    private static Map getIdMap(Map flatJsonLd) {
        Map idMap = [:]
        if (flatJsonLd.containsKey(GRAPH_KEY)) {
            def graphObject = flatJsonLd.get(GRAPH_KEY)
            // we expect this to be a list
            for (item in graphObject) {
                idMap = idMap + getIdMapRecursively(item)
            }
        }
        return idMap
    }

    private static Map getIdMapRecursively(Object thing) {
        if (thing instanceof List) {
            return getIdMapFromList(thing)
        } else if (thing instanceof Map) {
            return getIdMapFromMap(thing)
        } else {
            throw new FramingException(
                "Unexpected structure in flat JSON-LD: ${thing}")
        }
    }

    private static Map getIdMapFromList(List objects) {
        Map idMap = [:]

        for (object in objects) {
            idMap = idMap + getIdMapRecursively(object)
        }

        return idMap
    }

    private static Map getIdMapFromMap(Map item) {
        Map idMap = [:]

        if (item.containsKey(GRAPH_KEY)) {
            idMap = idMap + getIdMapRecursively(item.get(GRAPH_KEY))
        } else if (item.containsKey(ID_KEY)) {
            def id = item.get(ID_KEY)
            if (idMap.containsKey(id)) {
                Map existing = idMap.get(id)
                idMap.put(id, existing + item)
            } else {
                idMap.put(id, item)
            }
        }

        return idMap
    }

    static boolean validateItemModel(Document doc) {
        if (!doc || !doc.data) {
            throw new ModelValidationException("Document has no data to validate.")
        }

        // The real test of the "Item Model" is whether or not the supplied
        // document can be converted into some kind of correct(ish) MARC.

        MarcRecord marcRecord
        try {
            Document convertedDocument = converter.convert(doc.data, doc.id)
            String convertedText = (String) convertedDocument.data.get("content")
            marcRecord = MarcXmlRecordReader.fromXml(convertedText)
        } catch (Throwable e) {
            // Catch _everything_ that could go wrong with the convert() call,
            // including Asserts (Errors)
            return false
        }

        // Do some basic sanity checking on the resulting MARC holdings post.

        // Holdings posts must have 32 positions in 008
        for (Controlfield field008 : marcRecord.getControlfields("008")) {
            if (field008.getData().length() != 32) {
                return false
            }
        }

        // Holdings posts must have (at least one) 852 $b (sigel)
        boolean containsSigel = false
        for (Datafield field852 : marcRecord.getDatafields("852")) {
            if (field852.getSubfields("b").size() > 0) {
                containsSigel = true
                break
            }
        }
        if (!containsSigel) {
            return false
        }

        return true
    }
}

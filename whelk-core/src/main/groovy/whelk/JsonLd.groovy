package whelk

import org.codehaus.jackson.map.ObjectMapper
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.Datafield
import whelk.exception.FramingException
import whelk.exception.ModelValidationException

import se.kb.libris.util.marc.io.MarcXmlRecordReader
import se.kb.libris.util.marc.MarcRecord
import whelk.util.PropertyLoader

public class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String CONTEXT_KEY = "@context"
    static final String VOCAB_KEY = "@vocab"
    //static final String VALUE_KEY = "@base"
    static final String ID_KEY = "@id"
    static final String TYPE_KEY = "@type"
    //static final String VALUE_KEY = "@value"
    static final String LANGUAGE_KEY = "@language"
    static final String CONTAINER_KEY = "@container"
    //static final String VALUE_KEY = "@index"
    //static final String VALUE_KEY = "@list"
    //static final String VALUE_KEY = "@set"
    static final String REVERSE_KEY = "@reverse"
    static final String THING_KEY = "mainEntity"
    static final String WORK_KEY = "instanceOf"
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

    static final ObjectMapper mapper = new ObjectMapper()

    private static Logger log = LogManager.getLogger(JsonLd.class)

    Map<String, Map> context = [:]
    Map displayData
    Map vocabIndex
    private Map superClassOf
    private Map<String, Set> subClassesByType
    private String vocabId

    Set forcedSetTerms

    /**
     * Make an instance to incapsulate model driven behaviour.
     */
    JsonLd(Map contextData, Map displayData, Map vocabData) {
        setSupportData(contextData, displayData, vocabData)
    }

    void setSupportData(Map contextData, Map displayData, Map vocabData) {
        def contextObj = contextData[CONTEXT_KEY]
        if (contextObj instanceof List) {
            contextObj.each {
                context.putAll(it)
            }
        } else {
            context = (Map) contextObj
        }
        this.displayData = displayData ?: Collections.emptyMap()
        vocabId = context.get(VOCAB_KEY)

        vocabIndex = vocabData ?
            vocabData[JsonLd.GRAPH_KEY].collectEntries {
                [toTermKey(it[JsonLd.ID_KEY]), it]
            }
            : Collections.emptyMap()

        subClassesByType = new HashMap<String, Set>()

        generateSubClassesLists()

        expandAliasesInLensProperties()

        loadForcedSetTerms()
    }

    private void expandAliasesInLensProperties() {
        Map propAliases = [:]
        for (ctx in [displayData.get(CONTEXT_KEY), context]) {
            ctx.each { k, v ->
                if (v instanceof Map && v[CONTAINER_KEY] == LANGUAGE_KEY) {
                    propAliases[v[ID_KEY]] = k
                }
            }
        }
        displayData['lensGroups']?.values().each { group ->
            group.get('lenses')?.values().each { lens ->
                lens['showProperties'] = lens['showProperties'].collect {
                    def alias = propAliases[it]
                    return alias ? [it, alias] : it
                }.flatten()
            }
        }
    }

    String toTermKey(String termId) {
        return termId.replace(vocabId, '')
    }

    List expandLinks(List refs) {
        return JsonLd.expandLinks(refs, displayData[JsonLd.CONTEXT_KEY])
    }

    String expand(String ref) {
        return JsonLd.expand(ref, displayData[JsonLd.CONTEXT_KEY])
    }

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

    private static Object storeFlattened(Object current, result) {
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

    private static Map makeFlat(obj, result) {
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
        return refs.collect { expand(it, context) }
    }

    static String expand(String ref, Map context) {
        def match
        if (ref =~ $/^https?:///$) {
            return ref
        } else if ((match = ref =~ /^([a-z0-9]+):(.*)$/)) {
            def resolved = context[match[0][1]]
            if (resolved) {
                URI base = new URI(resolved)
                return base.resolve(match[0][2]).toString()
            }
        }
        return ref
    }

    private static Set getLocalObjects(Map jsonLd) {
        Set result = [] as Set
        if (jsonLd.get(GRAPH_KEY)) {
            // we expect this to be a list
            for (item in jsonLd.get(GRAPH_KEY)) {
                result.addAll(getLocalObjectsRecursively(item))
            }
        }
        return result
    }

    private static Set getLocalObjectsRecursively(Object thing){
        if (thing instanceof List) {
            return getLocalObjectsFromList(thing)
        } else if (thing instanceof Map) {
            return getLocalObjectsFromMap(thing)
        } else {
            throw new FramingException(
                "Unexpected structure in JSON-LD: ${thing}")
        }
    }

    private static Set getLocalObjectsFromList(List things) {
        Set result = [] as Set

        for (thing in things) {
            result.addAll(getLocalObjectsRecursively(thing))
        }

        return result
    }

    private static Set getLocalObjectsFromMap(Map jsonLd) {
        Set result = [] as Set
        if (jsonLd.containsKey(GRAPH_KEY)) {
            def thing = jsonLd.get(GRAPH_KEY)
            result.addAll(getLocalObjectsRecursively(thing))
        }

        if (jsonLd.containsKey(ID_KEY)) {
            def id = jsonLd.get(ID_KEY)
            if (!result.contains(id)) {
                result << id
            }
        }

        if (jsonLd.containsKey(JSONLD_ALT_ID_KEY)) {
            jsonLd.get(JSONLD_ALT_ID_KEY).each {
                if (!it.containsKey(ID_KEY)) {
                    return
                }

                def id = it.get(ID_KEY)
                if (!result.contains(id)) {
                    result << id
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

    Map embellish(Map jsonLd, Map additionalObjects, boolean filterOutNonChipTerms = true) {
        if (!jsonLd.get(GRAPH_KEY)) {
            return jsonLd
        }

        List graphItems = jsonLd.get(GRAPH_KEY)

        if (filterOutNonChipTerms) {
            additionalObjects.each { id, object ->
                Map chip = toChip(object)
                if (chip.containsKey('@graph')) {
                    graphItems << chip
                } else {
                    graphItems << ['@graph': chip]
                }
            }
        } else {
            additionalObjects.each { id, object ->
                if (object instanceof Map) {
                    if (object.containsKey('@graph')) {
                        graphItems << object
                    } else {
                        graphItems << ['@graph': object]
                    }
                }
            }
        }

        jsonLd[GRAPH_KEY] = graphItems

        return jsonLd
    }

    /**
     * Convert a post to card.
     *
     */
    public Map toCard(Map thing) {
        Map lensGroups = displayData.get("lensGroups")
        Map cardLensGroup = lensGroups.get("cards")
        Map result = [:]

        Map card = removeProperties(thing, cardLensGroup)
        card.each {key, value ->
            result[key] = toChip(value)
        }
        return result
    }

    /**
     * Convert a post to chip.
     *
     */
    public Object toChip(Object object) {
        Map lensGroups = displayData.get("lensGroups")
        Map chipLensGroup = lensGroups.get("chips")
        Map itemsToKeep = [:]
        Map result = [:]

        if (object instanceof List) {
            return object.collect { toChip(it) }
        } else if ((object instanceof Map)) {
            itemsToKeep = removeProperties(object, chipLensGroup)
            itemsToKeep.each {key, value ->
                result[key] = toChip(value)
            }
            return result
        } else {
            return object
        }
    }

    private Map removeProperties(Map thing, Map lensGroup) {
        Map itemsToKeep = [:]

        Map lens = getLensFor(thing, lensGroup)

        if (lens) {
            List propertiesToKeep = lens.get("showProperties")

            thing.each {key, value ->
                if (shouldKeep(key, propertiesToKeep)) {
                    itemsToKeep[key] = value
                }
            }
            return itemsToKeep
        } else {
            return thing
        }
    }

    Map getLensFor(Map thing, Map lensGroup) {
        def types = thing.get(TYPE_KEY)
        if (types instanceof String)
            types = [types]
        for (type in types) {
            return findLensForType(type, lensGroup)
                    ?: findLensForType('Resource', lensGroup)
        }
    }

    private Map findLensForType(String typeKey, Map lensGroup) {
        def lenses = lensGroup['lenses']
        def lens = lenses.get(typeKey)
        if (lens)
            return lens
        def typedfn = vocabIndex.get(typeKey)
        if (!typedfn)
            return null
        def basetypes = typedfn.get('subClassOf')
        if (basetypes instanceof Map)
            basetypes = [basetypes]
        for (basetype in basetypes) {
            if (!basetype[ID_KEY])
                continue
            def baseTypeKey = toTermKey(basetype[ID_KEY])
            lens = findLensForType(baseTypeKey, lensGroup)
            if (lens)
                return lens
        }
        return null
    }

    private static boolean shouldKeep(String key, List propertiesToKeep) {
        return (key in propertiesToKeep || key.startsWith("@"))
    }

    private static Map getIdMap(Map data) {
        Map idMap = new HashMap()
        populateIdMap(data, idMap)
        return idMap
    }

    private static void populateIdMap(Map data, Map idMap) {
        for (Object key : data.keySet()) {

            if (key.equals("@id") && data.keySet().size() > 1)
                idMap.put(data.get(key), data)

            Object obj = data.get(key)
            if (obj instanceof List)
                populateIdMap( (List) obj, idMap )
            else if (obj instanceof Map)
                populateIdMap( (Map) obj, idMap )
        }
    }

    private static void populateIdMap(List data, Map idMap) {
        for (Object element : data) {
            if (element instanceof List)
                populateIdMap( (List) element, idMap )
            else if (element instanceof Map)
                populateIdMap( (Map) element, idMap )
        }
    }

    private static void assembleFramed(Map currentNode, Map idMap, Set passedIDs) {
        String id = currentNode.get("@id")
        if (id != null && !passedIDs.contains(id)) {
            passedIDs.add(id)
            Map object = (Map) idMap.get(id)
            if (object != null) {
                currentNode.clear()
                currentNode.putAll( (Map) Document.deepCopy(object) )
            }
        }

        for (Object key : currentNode.keySet()) {
            Object object = currentNode.get(key)
            if (object instanceof Map)
                assembleFramed( (Map) object, idMap, passedIDs )
            else if (object instanceof List)
                assembleFramed( (List) object, idMap, passedIDs )
        }

    }

    private static void assembleFramed(List list, Map idMap, Set passedIDs){
        for (Object element: list) {
            if (element instanceof Map)
                assembleFramed((Map) element, idMap, passedIDs)
            else if (element instanceof List)
                assembleFramed((List) element, idMap, passedIDs)
        }
    }

    public static Map frame(String mainId, Map originalData) {
        if (mainId)
            mainId = Document.BASE_URI.resolve(mainId)
        else
            return originalData

        Map idMap = getIdMap(originalData)

        // preamble
        HashMap mainObject = new HashMap()
        mainObject.put("@id", mainId)

        // assemble
        assembleFramed(mainObject, idMap, new HashSet())

        // clean up
        Set referencedBNodes = new HashSet()
        getReferencedBNodes(mainObject, referencedBNodes)
        cleanUnreferencedBNodeIDs(mainObject, referencedBNodes)


        return mainObject
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

    /*
    // TODO: This doesn't belong here, but in a validation service. At least
    // while dependent on a JsonLD2MarcXMLConverter. If validation worked on
    // the JSON-LD, using vocab and context data, it would be more
    // appropriate here.
    //import whelk.converter.marc.JsonLD2MarcXMLConverter
    //static final JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter()
    @Deprecated
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
    */

    public void getSuperClasses(String type, List<String> result) {
        def termMap = vocabIndex[type]
        if (termMap == null)
            return

        if (termMap["subClassOf"] != null) {
            List superClasses = termMap["subClassOf"]

            for (superClass in superClasses) {
                if (superClass == null || superClass["@id"] == null) {
                    continue
                }
                String superClassType = toTermKey( superClass["@id"] )
                result.add(superClassType)
                getSuperClasses(superClassType, result)
            }
        }
    }

    private generateSubClassesLists() {
        superClassOf = [:]
        for (String type : vocabIndex.keySet()) {
            def termMap = vocabIndex[type]
            def superClasses = termMap["subClassOf"]

            // Make list if not list already.
            if (!(superClasses instanceof List))
                superClasses = [superClasses]

            for (superClass in superClasses) {
                if (superClass == null || superClass["@id"] == null) {
                    continue
                }

                String superClassType = toTermKey( superClass["@id"] )
                if (superClassOf[superClassType] == null)
                    superClassOf[superClassType] = []
                superClassOf[superClassType].add(type)
            }
        }
    }

    public boolean isSubClassOf(String type, String baseType) {
        if (type == baseType)
            return true
        Set<String> bases = getSubClasses(baseType)
        return type in bases
    }

    public Set<String> getSubClasses(String type) {
        Set<String> subClasses = subClassesByType[type]
        if (subClasses.is(null)) {
            subClasses = new HashSet<String>()
            getSubClasses(type, subClasses)
            subClassesByType[type] = subClasses
        }
        return subClasses
    }

    public void getSubClasses(String type, Collection<String> result) {
        if (type == null)
            return

        def subClasses = superClassOf[type]
        if (subClasses == null)
            return

        result.addAll(subClasses)

        for (String subClass : subClasses) {
            getSubClasses(subClass, result)
        }
    }

    private void loadForcedSetTerms()
            throws IOException
    {
        /*
        forcedNoSetTerms are those that are used at some point with property/link (as opposed to addProperty/addLink).
        The intersection of forcedNoSetTerms and forcedSetTerms are in conflict, dealing with these remains an issue.
         */
        Set forcedNoSetTerms = new HashSet<>()
        forcedSetTerms = new HashSet<>()

        InputStream marcFrameStream = getClass().getClassLoader().getResourceAsStream("ext/marcframe.json")

        ObjectMapper mapper = new ObjectMapper()
        Map marcFrame = mapper.readValue(marcFrameStream, HashMap.class)
        parseForcedSetTerms(marcFrame, forcedNoSetTerms)

        // As an interim solution conflicted terms are considered no-set-terms.
        forcedSetTerms.removeAll(forcedNoSetTerms)
    }

    private void parseForcedSetTerms(Map marcFrame, Set forcedNoSetTerms) {
        for (Object key : marcFrame.keySet()) {
            Object value = marcFrame.get(key)
            if ( (key.equals("addLink") || key.equals("addProperty")) && value instanceof String )
                forcedSetTerms.add((String) value)

            if (value instanceof Map)
                parseForcedSetTerms( (Map) value, forcedNoSetTerms )
            if (value instanceof List)
                parseForcedSetTerms( (List) value, forcedNoSetTerms )
        }
    }

    private void parseForcedSetTerms(List marcFrame, Set forcedNoSetTerms) {
        for (Object entry : marcFrame) {
            if (entry instanceof Map)
                parseForcedSetTerms( (Map) entry, forcedNoSetTerms )
            if (entry instanceof List)
                parseForcedSetTerms( (List) entry, forcedNoSetTerms )
        }
    }
}

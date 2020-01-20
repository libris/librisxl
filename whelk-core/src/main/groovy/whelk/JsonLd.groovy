package whelk

import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.codehaus.jackson.map.ObjectMapper
import whelk.exception.FramingException
import whelk.exception.WhelkRuntimeException

import java.util.regex.Matcher

@CompileStatic
class JsonLd {

    static final String GRAPH_KEY = "@graph"
    static final String CONTEXT_KEY = "@context"
    static final String VOCAB_KEY = "@vocab"
    static final String ID_KEY = "@id"
    static final String TYPE_KEY = "@type"
    static final String LANGUAGE_KEY = "@language"
    static final String CONTAINER_KEY = "@container"
    static final String SET_KEY = "@set"
    static final String LIST_KEY = "@list"
    static final String REVERSE_KEY = "@reverse"
    // JSON-LD 1.1
    static final String PREFIX_KEY = "@prefix"

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

    static final String SEARCH_KEY = "_str"

    static final List<String> NS_SEPARATORS = ['#', '/', ':']

    static final List<String> NON_DEPENDANT_RELATIONS = ['narrower', 'broader', 'expressionOf', 'related', 'derivedFrom']

    static final Set<String> LD_KEYS

    static final ObjectMapper mapper = new ObjectMapper()

    static {
        LD_KEYS = [
            GRAPH_KEY,
            CONTEXT_KEY,
            VOCAB_KEY,
            ID_KEY,
            TYPE_KEY,
            LANGUAGE_KEY,
            CONTAINER_KEY,
            SET_KEY,
            LIST_KEY,
            REVERSE_KEY
        ] as Set
    }

    private static Logger log = LogManager.getLogger(JsonLd.class)

    Map<String, Map> context = [:]
    Map displayData
    Map vocabIndex

    List<String> locales
    private String vocabId
    private Map<String, String> nsToPrefixMap = [:]

    private Map<String, List<String>> superClassOf
    private Map<String, Set<String>> subClassesByType
    private Map<String, List<String>> superPropertyOf
    private Map<String, Set<String>> subPropertiesByType

    Map langContainerAlias = [:]

    /**
     * This includes terms that are declared as either set or list containers
     * in the context.
     */
    Set<String> repeatableTerms

    /**
     * Make an instance to encapsulate model driven behaviour.
     */
    JsonLd(Map contextData, Map displayData, Map vocabData,
            List<String> locales = ['sv', 'en']) {
        setSupportData(contextData, displayData, vocabData)
        this.locales = locales
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    void setSupportData(Map contextData, Map displayData, Map vocabData) {
        def contextObj = contextData[CONTEXT_KEY]
        if (contextObj instanceof List) {
            contextObj.each { context.putAll(it) }
        } else if (contextObj) {
            context.putAll(contextObj)
        }

        repeatableTerms = context.findResults { key, value ->
            if (isSetContainer(value) || isListContainer(value))
                return key
        } as Set<String>

        this.displayData = displayData ?: Collections.emptyMap()

        setupPrefixes()

        vocabId = context.get(VOCAB_KEY)

        vocabIndex = vocabData ?
                vocabData[GRAPH_KEY].collectEntries {
                [toTermKey((String) it[ID_KEY]), it]
            }
            : Collections.emptyMap()

        subClassesByType = new HashMap<String, Set>()
        superClassOf = generateSubTermLists("subClassOf")

        subPropertiesByType = new HashMap<String, Set>()
        superPropertyOf = generateSubTermLists("subPropertyOf")

        buildLangContainerAliasMap()

        expandAliasesInLensProperties()
        expandInheritedLensProperties()
    }

    private void setupPrefixes() {
        context.each { String pfx, dfn ->
            def term = dfn instanceof Map &&
                       dfn[PREFIX_KEY] == true ? dfn[ID_KEY] : dfn
            if (term instanceof String) {
                String ns = (String) term
                if (NS_SEPARATORS.any { ns.endsWith(it) }) {
                    nsToPrefixMap[ns] = pfx
                }
            }
        }

    }

    @TypeChecked(TypeCheckingMode.SKIP)
    private void buildLangContainerAliasMap() {
        for (ctx in [displayData.get(CONTEXT_KEY), context]) {
            ctx.each { k, v ->
                if (isLangContainer(v)) {
                    langContainerAlias[v[ID_KEY]] = k
                }
            }
        }
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    private void expandAliasesInLensProperties() {
        displayData['lensGroups']?.values().each { group ->
            group.get('lenses')?.values().each { lens ->
                lens['showProperties'] = lens['showProperties'].collect {
                    def alias = langContainerAlias[it]
                    return alias ? [it, alias] : it
                }.flatten()
            }
        }
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    expandInheritedLensProperties() {
        def lensesById = [:]
        displayData['lensGroups']?.values()?.each { group ->
            group.get('lenses')?.values()?.each { lens ->
                lensesById[lens['@id']] = lens
            }
        }

        def flattenedProps
        flattenedProps = { lens, hierarchy=[] ->
            if (hierarchy.contains(lens['@id'])) {
                throw new FresnelException('fresnel:extends inheritance loop: ' + hierarchy.toString())
            }

            String superLensId = lens.get('fresnel:extends')?.get('@id')
            if (!superLensId) {
                return lens['showProperties']
            }
            else {
                if (!lensesById[superLensId]) {
                    throw new FresnelException("Super lens not found: ${lens['@id']} fresnel:extends ${superLensId}")
                }

                def superProps = flattenedProps(lensesById[superLensId], hierarchy << lens['@id'])
                def props = lens['showProperties']
                if(!props.contains('fresnel:super')) {
                    props = ['fresnel:super'] + props
                }
                return props.collect { it == 'fresnel:super' ? superProps : it }.flatten()
            }
        }

        lensesById.values().each { Map lens ->
            lens.put('showProperties', flattenedProps(lens))
            lens.remove('fresnel:extends')
        }
    }

    boolean isSetContainer(dfn) {
        return dfn instanceof Map && dfn[CONTAINER_KEY] == SET_KEY
    }

    boolean isListContainer(dfn) {
        return dfn instanceof Map && dfn[CONTAINER_KEY] == LIST_KEY
    }

    boolean isLangContainer(dfn) {
        return dfn instanceof Map && dfn[CONTAINER_KEY] == LANGUAGE_KEY
    }

    String toTermKey(String termId) {
        Integer splitPos = NS_SEPARATORS.findResult {
            int idx = termId.lastIndexOf(it)
            return idx > -1 ? idx + 1 : null
        }
        if (splitPos == null) {
            return termId
        }

        String baseNs = termId.substring(0, splitPos)
        String term = termId.substring(splitPos)

        if (baseNs == vocabId) {
            return term
        }

        String pfx = nsToPrefixMap[baseNs]
        if (pfx) {
            return pfx + ':' + term
        }

        return termId.replace(vocabId, '')
    }

    List expandLinks(List refs) {
        return expandLinks(refs, (Map) displayData[CONTEXT_KEY])
    }

    String expand(String ref) {
        return expand(ref, (Map) displayData[CONTEXT_KEY])
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
                foundIdentifier = ((Map)((List)jsonLd.get(GRAPH_KEY)).first()).get(ID_KEY)
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

    static List getExternalReferences(Map jsonLd){
        Set allReferences = getAllReferences(jsonLd)
        Set localObjects = getLocalObjects(jsonLd)
        List externalRefs = allReferences.minus(localObjects) as List
        // NOTE: this is necessary because some documents contain references to
        // bnodes that don't exist (in that document).
        return filterOutDanglingBnodes(externalRefs)
    }

    static List expandLinks(List refs, Map context) {
        return refs.collect { expand( (String) it, context) }
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    static String expand(String ref, Map context) {
        if (ref =~ $/^https?:///$) {
            return ref
        } else {
            Matcher match = ref =~ /^([a-z0-9]+):(.*)$/
            if (match) {
                def resolved = context[match[0][1]]
                if (resolved) {
                    URI base = new URI(resolved)
                    return base.resolve(match[0][2]).toString()
                }
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

        return result
    }

    private static List filterOutDanglingBnodes(List refs) {
        return refs.findAll {
            !((String)it).startsWith('_:')
        }
    }

    static Set getAllReferences(Map jsonLd) {
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
            return new HashSet()
        }
    }

    private static Set getAllReferencesFromMap(Map item) {
        Set refs = []

        if (isReference(item)) {
            refs.add(item[ID_KEY])
            return refs
        } else {
            item.each { key, value ->
                if (key != JSONLD_ALT_ID_KEY) {
                    refs << getRefs(value)
                }
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


    //==== Utils ====

    Set validate(Map obj) {
        Set<String> errors = new LinkedHashSet<>()
        doValidate(obj, errors)
        return errors
    }

    private void doValidate(Map obj, Set errors) {
        for (Object keyObj : obj.keySet()) {
            if (!(keyObj instanceof String)) {
                errors << "Invalid key: $keyObj"
                continue
            }

            String key = (String) keyObj
            Object value = obj[key]

            if ((key == ID_KEY || key == TYPE_KEY)
                && !(value instanceof String)) {
                errors << "Unexpected value of $key: ${value}"
                continue
            }

            Map termDfn = vocabIndex[key] instanceof Map ? vocabIndex[key] : null
            if (!termDfn && key.indexOf(':') > -1) {
                termDfn = vocabIndex[expand(key)]
            }

            Map ctxDfn = context[key] instanceof Map ? context[key] : null
            boolean isVocabTerm = ctxDfn && ctxDfn[TYPE_KEY] == VOCAB_KEY

            if (!termDfn && !LD_KEYS.contains(key)) {
                errors << "Unknown term: $key"
            }

            if ((key == TYPE_KEY || isVocabTerm)
                && !vocabIndex.containsKey((String) value)) {
                errors << "Unknown vocab value for $key: $value"
            }

            boolean expectRepeat = key == GRAPH_KEY || key in repeatableTerms
            boolean isRepeated = value instanceof List
            if (expectRepeat && !isRepeated) {
                errors << "Expected $key to be an array."
            } else if (!expectRepeat && isRepeated) {
                errors << "Unexpected array for $key."
            }

            List valueList = isRepeated ? (List) value : null
            if (valueList && valueList.size() == 0) {
                continue
            }
            Object firstValue = valueList?.getAt(0) ?: value
            boolean valueIsObject = firstValue instanceof Map

            if (firstValue && termDfn
                    && termDfn[TYPE_KEY] == 'ObjectProperty') {
                if (!isVocabTerm && !valueIsObject) {
                    errors << "Expected value type of $key to be object (value: $value)."
                } else if (isVocabTerm && valueIsObject) {
                    errors << "Expected value type of $key to be a vocab string (value: $value)."
                }
            }

            if (value instanceof List) {
                value.each {
                    if (it instanceof Map) {
                        doValidate((Map) it, errors)
                    }
                }
            } else if (value instanceof Map) {
                doValidate((Map) value, errors)
            }
        }
    }

    boolean softMerge(Map<String, Object> obj, Map<String, Object> into) {
        if (obj == null || into == null) {
            return false
        }
        for (String key : obj.keySet()) {
            if (key == TYPE_KEY) {
                def objType = obj[TYPE_KEY]
                def intoType = into[TYPE_KEY]
                // TODO: handle if type is List ...
                if (objType instanceof Collection
                    || intoType instanceof Collection) {
                    return false
                }
                if (objType instanceof String && intoType instanceof String
                    && !isSubClassOf((String) objType, (String) intoType)) {
                    return false
                }
            } else {
                def val = obj[key]
                def intoval = into[key]
                if (intoval && val != intoval) {
                    return false
                }
            }
        }
        into.putAll(obj)
        return true
    }

    static List<List<String>> findPaths(Map obj, String key, String value) {
        List paths = []
        new DFS().search(obj, { List path, v ->
            if (value == v && key == path[-1]) {
                paths << new ArrayList(path)
            }
        })
        return paths
    }

    private static class DFS {
        interface Callback {
            void node(List path, value)
        }

        List path = []
        Callback cb

        void search(obj, Callback callback) {
            cb = callback
            path = []
            node(obj)
        }

        private void node(obj) {
            cb.node(path, obj)
            if (obj instanceof Map) {
                descend(((Map) obj).entrySet().collect({ new Tuple2(it.value, it.key) }))
            } else if (obj instanceof List) {
                descend(((List) obj).withIndex())
            }
        }

        private void descend(List<Tuple2> nodes) {
            for (n in nodes) {
                path << n.second
                node(n.first)
                path.remove(path.size()-1)
            }
        }
    }



    //==== Class-hierarchies ====

    void getSuperClasses(String type, List<String> result) {
        def termMap = vocabIndex[type]
        if (termMap == null)
            return

        if (termMap["subClassOf"] != null) {
            List superClasses = (List) termMap["subClassOf"]

            for (superClass in superClasses) {
                if (superClass == null || superClass[ID_KEY] == null) {
                    continue
                }
                String superClassType = toTermKey((String) superClass[ID_KEY])
                result.add(superClassType)
                getSuperClasses(superClassType, result)
            }
        }
    }

    private Map<String, List<String>> generateSubTermLists(String relationToSuper) {
        def superTermOf = [:]
        for (String type : vocabIndex.keySet()) {
            def termMap = vocabIndex[type]
            def superTerms = termMap[relationToSuper]

            // Make list if not list already.
            if (!(superTerms instanceof List))
                superTerms = [superTerms]

            for (superTerm in superTerms) {
                if (superTerm == null || superTerm[ID_KEY] == null) {
                    continue
                }

                String superTermType = toTermKey((String) superTerm[ID_KEY])
                if (superTermOf[superTermType] == null)
                    superTermOf[superTermType] = []
                ((List)superTermOf[superTermType]).add(type)
            }
        }
        return superTermOf
    }

    boolean isSubClassOf(String type, String baseType) {
        if (type == baseType)
            return true
        Set<String> bases = getSubClasses(baseType)
        return type in bases
    }

    Set<String> getSubClasses(String type) {
        return getSubTerms(type, superClassOf, subClassesByType)
    }

    Set<String> getSubProperties(String type) {
        return getSubTerms(type, superPropertyOf, subPropertiesByType)
    }

    private Set<String> getSubTerms(String type,
            Map<String, List<String>> superTermOf,
            Map<String, Set<String>> cache) {
        Set<String> subTerms = cache[type]
        if (subTerms.is(null)) {
            subTerms = new HashSet<String>()
            collectSubTerms(type, superTermOf, subTerms)
            cache[type] = subTerms
        }
        return subTerms
    }

    private void collectSubTerms(String type,
            Map<String, List<String>> superTermOf,
            Collection<String> result) {
        if (type == null)
            return

        List subTerms = (List) (superTermOf[type])
        if (subTerms == null)
            return

        result.addAll(subTerms)

        for (String subTerm : subTerms) {
            collectSubTerms(subTerm, superTermOf, result)
        }
    }


    //==== Embellish ====

    Map embellish(Map jsonLd, Iterable additionalObjects, boolean filterOutNonChipTerms = true) {
        if (!jsonLd.get(GRAPH_KEY)) {
            return jsonLd
        }

        List graphItems = jsonLd.get(GRAPH_KEY)

        if (filterOutNonChipTerms) {
            additionalObjects.each { object ->
                Map chip = (Map) toChip(object)
                if (chip.containsKey('@graph')) {
                    graphItems << chip
                } else {
                    graphItems << ['@graph': chip]
                }
            }
        } else {
            additionalObjects.each { object ->
                if (object instanceof Map) {
                    if (((Map)object).containsKey('@graph')) {
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

    Map toCard(Map thing, List<List> preservePaths) {
        return toCard(thing, true, false, false, preservePaths)
    }

    Map toCard(Map thing, boolean chipsify = true, boolean addSearchKey = false,
            boolean reduceKey = false, List<List> preservePaths = []) {
        Map result = [:]

        Map card = removeProperties(thing, 'cards')
        // If result is too small, use chip instead.
        // TODO: Support and use extends + super in card defs instead.)
        if (card.size() < 2) {
            card = removeProperties(thing, 'chips')
        }

        restorePreserved(card, thing, preservePaths)

        reduceKey = reduceKey ?: { isSubClassOf((String) it, 'StructuredValue') }

        card.each { key, value ->
            def lensValue = value
            if (chipsify) {
                lensValue = toChip(value, pathRemainders([key], preservePaths))
            } else {
                if (value instanceof List) {
                    lensValue = ((List) value).withIndex().collect { it, index ->
                        it instanceof Map
                        ? toCard((Map) it, chipsify, addSearchKey, reduceKey, pathRemainders([index, key], preservePaths))
                        : it
                    }
                } else if (value instanceof Map) {
                    lensValue = toCard((Map) value, chipsify, addSearchKey, reduceKey, pathRemainders([key], preservePaths))
                }
            }
            result[key] = lensValue
        }

        if (addSearchKey) {
            List key = makeSearchKeyParts(card)
            if (reduceKey) {
                for (v in result.values()) {
                    if (v instanceof List && ((List)v).size() == 1) {
                        v = ((List)v)[0]
                    }
                    if (v instanceof Map) {
                        if (v[SEARCH_KEY]) {
                            key << ((Map)v).remove(SEARCH_KEY)
                        }
                    }
                }
            }
            if (key) {
                result[SEARCH_KEY] = key.join(' ')
            }
        }

        return result
    }

    Object toChip(Object object, List<List> preservePaths = []) {
        if (object instanceof List) {
            return object.withIndex().collect { it, ix ->
                toChip(it, pathRemainders([ix], preservePaths))
            }
        } else if ((object instanceof Map)) {
            Map result = [:]
            Map reduced = removeProperties(object, 'chips')
            restorePreserved(reduced, (Map) object, preservePaths)
            reduced.each { key, value ->
                result[key] = toChip(value, pathRemainders([key], preservePaths))
            }
            return result
        } else {
            return object
        }
    }

    List makeSearchKeyParts(Map object) {
        Map lensGroups = displayData.get('lensGroups')
        Map lensGroup = lensGroups.get('chips')
        Map lens = getLensFor(object, lensGroup)
        List parts = []
        def type = object.get(TYPE_KEY)
        // TODO: a bit too hard-coded...
        if (type instanceof String && type != 'Identifier' && isSubClassOf(type, 'Identifier')) {
            parts << type
        }
        if (lens) {
            List propertiesToKeep = (List) lens.get("showProperties")
            for (prop in propertiesToKeep) {
                def values = object[prop]
                if (isLangContainer(context[prop]) && values instanceof Map) {
                    values = locales.findResult { values[it] }
                }
                if (!(values instanceof List)) {
                    values = values ? [values] : []
                }
                // TODO: find recursively (up to a point)? For what? Only
                // StructuredValue? Or if a chip property value is a
                // StructuredValue? (Use 'tokens' if available...)
                for (value in values) {
                    if (value instanceof String) {
                        // TODO: marc:nonfilingChars?
                        parts << value
                    }
                }
            }
        }
        return parts
    }

    private static void restorePreserved(Map cardOrChip, Map thing, List<List> preservePaths) {
        preservePaths.each {
            if (!it.isEmpty()) {
                def key = it[0]
                if (thing.containsKey(key) && !cardOrChip.containsKey(key)) {
                    cardOrChip[key] = thing[key]
                }
            }
        }
    }

    private static List<List> pathRemainders(List prefix, List<List> paths) {
        return paths
                .findAll{ it.size() >= prefix.size() && it.subList(0, prefix.size()) == prefix }
                .collect{ it.drop(prefix.size()) }
    }

    private Map removeProperties(Map thing, String lensType) {
        Map lensGroups = displayData.get('lensGroups')
        Map lensGroup = lensGroups.get(lensType)
        Map lens = getLensFor(thing, lensGroup)

        Map result = [:]
        if (lens) {
            List propertiesToKeep = (List) lens.get("showProperties")

            thing.each { key, value ->
                if (shouldKeep((String) key, (List) propertiesToKeep)) {
                    result[key] = value
                }
            }
            return result
        } else {
            return thing
        }
    }

    Map getLensFor(Map thing, Map lensGroup) {
        def types = thing.get(TYPE_KEY)
        if (types instanceof String)
            types = [types]
        for (type in types) {
            Map lensForType = findLensForType((String)type, lensGroup)
            if (lensForType)
                return lensForType
            return findLensForType('Resource', lensGroup)
        }
    }

    private Map findLensForType(String typeKey, Map lensGroup) {
        def lenses = lensGroup['lenses']
        Map lens = ((Map)lenses).get(typeKey)
        if (lens)
            return lens
        def typedfn = vocabIndex.get(typeKey)
        if (!typedfn)
            return null
        def basetypes = ((Map)typedfn).get('subClassOf')
        if (basetypes instanceof Map)
            basetypes = [basetypes]
        for (basetype in basetypes) {
            if (!basetype[ID_KEY])
                continue
            def baseTypeKey = toTermKey((String)basetype[ID_KEY])
            lens = findLensForType(baseTypeKey, lensGroup)
            if (lens)
                return lens
        }
        return null
    }

    private static boolean shouldKeep(String key, List propertiesToKeep) {
        return (key == RECORD_KEY || key == THING_KEY ||
                key in propertiesToKeep || key.startsWith("@"))
    }


    //==== Flattening ====

    static boolean isFlat(Map jsonLd) {
        if ((jsonLd.containsKey(GRAPH_KEY) && jsonLd.get(GRAPH_KEY) instanceof List)) {
            return true
        }
        return false
    }

    /**
     * This flatten-method does not create description-based flat json
     * (i.e. with entry, items and quoted)
     *
     */
    static Map flatten(Map framedJsonLd) {
        if (isFlat(framedJsonLd) || !framedJsonLd.containsKey(ID_KEY)) {
            return framedJsonLd
        }

        def flatList = []

        storeFlattened(framedJsonLd, flatList)

        return [(GRAPH_KEY): flatList.reverse()]
    }

    private static Object storeFlattened(Object current, List result) {
        if (current instanceof Map) {
            def flattened = makeFlat(current, result)
            if (flattened.containsKey(ID_KEY) && flattened.size() > 1) {
                if (!result.contains(flattened)) {
                    result.add(flattened)
                }
            }
            def itemid = current.get(ID_KEY)
            return (itemid ? [(ID_KEY): itemid] : flattened)
        }
        return current
    }

    private static Map makeFlat(Map obj, List result) {
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


    //==== Framing ====

    static boolean isFramed(Map jsonLd) {
        if (jsonLd && !jsonLd.containsKey(GRAPH_KEY)) {
            return true
        }
        return false
    }

    static Map frame(String mainId, Map inData) {
        Map<String, Map> idMap = getIdMap(inData)

        putRecordReferencesIntoThings(idMap)

        Map mainItem = idMap[mainId]

        Map framedData
        try {
            framedData = embed(mainId, mainItem, idMap, new HashSet<String>())
            if (!framedData) {
                throw new FramingException("Failed to frame JSONLD ($inData)")
            }
        } catch (StackOverflowError sofe) {
            throw new FramingException("Unable to frame JSONLD ($inData). Recursive loop?)", sofe)
        }

        cleanUp(framedData)

        return framedData
    }

    static void putRecordReferencesIntoThings(Map<String, Map> idMap) {
        for (obj in idMap.values()) {
            Map thingRef = obj[THING_KEY]
            if (thingRef) {
                String thingId = thingRef[ID_KEY]
                Map thing = idMap[thingId]
                if (thing) {
                    Map recRef = [:]
                    recRef[ID_KEY] = obj[ID_KEY]
                    thing[RECORD_KEY] = recRef
                } else {
                    log.debug("Record <${obj[ID_KEY]}> is missing thing <${thingId}>.")
                }
            }
        }
    }

    private static Map embed(String mainId, Map mainItem, Map idMap, Set embedChain) {
        embedChain.add(mainId)
        Map newItem = [:]
        mainItem.each { key, value ->
            newItem.put(key, toEmbedded(value, idMap, embedChain))
        }
        return newItem
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
            Map obj = null
            String oId = o.get(ID_KEY)
            if (!oId) {
                obj = (Map) o
            } else if (!embedChain.contains(oId)) {
                Map fullObj = (Map) idMap.get(oId)
                obj = fullObj ? [:] + fullObj : null
            }
            if (obj) {
                return embed(oId, obj, idMap, new HashSet<String>(embedChain))
            }
        }
        return o
    }

    /*
     * Traverse the data and index all non-reference objects on their @id:s.
     */
    private static Map getIdMap(Map data) {
        Map idMap = new HashMap()
        populateIdMap(data, idMap)
        return idMap
    }

    private static void populateIdMap(Map data, Map idMap) {
        for (Object key : data.keySet()) {
            if (key.equals(ID_KEY)
                // Don't index references (i.e. objects with only an @id).
                && data.keySet().size() > 1
                // Don't index graphs, since their @id:s do not denote them.
                && !data.containsKey(GRAPH_KEY)
               ) {
                idMap.put(data.get(key), data)
                continue
            }
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

    private static void cleanUp(Map framedMap) {
        Set referencedBNodes = new HashSet()
        getReferencedBNodes(framedMap, referencedBNodes)
        cleanUnreferencedBNodeIDs(framedMap, referencedBNodes)
    }

    /**
     * Fills the referencedBNodes set with all "_:*" ids that are referenced
     * anywhere in the structure/document (and thus cannot be safely removed).
     */
    static void getReferencedBNodes(Map map, Set referencedBNodes) {
        // A jsonld reference is denoted as a json object containing exactly one member, with the key "@id".
        if (map.size() == 1) {
            String key = map.keySet().getAt(0)
            if (key.equals(ID_KEY)) {
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

    static void getReferencedBNodes(List list, Set referencedBNodes) {
        for (Object item : list) {
            if (item instanceof Map)
                getReferencedBNodes((Map) item, referencedBNodes)
        }
    }

    static void cleanUnreferencedBNodeIDs(Map map, Set referencedBNodes) {
        if (map.size() > 1) {
            if (map.containsKey(ID_KEY)) {
                String id = map.get(ID_KEY)

                if (id.startsWith("_:") && !referencedBNodes.contains(id)) {
                    map.remove(ID_KEY)
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

    static void cleanUnreferencedBNodeIDs(List list, Set referencedBNodes) {
        for (Object item : list) {
            if (item instanceof Map)
                cleanUnreferencedBNodeIDs((Map) item, referencedBNodes)
        }
    }

    class FresnelException extends WhelkRuntimeException {
        FresnelException(String msg) {
            super(msg);
        }
    }
}

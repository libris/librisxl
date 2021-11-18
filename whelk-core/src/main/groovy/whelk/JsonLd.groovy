package whelk

import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import whelk.exception.FramingException
import whelk.exception.WhelkRuntimeException
import whelk.util.DocumentUtil

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
    static final String RECORD_STATUS_KEY = "recordStatus"
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
    
    static final String CACHE_RECORD_TYPE = 'CacheRecord'
    static final String PLACEHOLDER_RECORD_TYPE = 'PlaceholderRecord'
    static final String PLACEHOLDER_ENTITY_TYPE = 'Resource'

    static final String SEARCH_KEY = "_str"

    static final List<String> NS_SEPARATORS = ['#', '/', ':']

    static final List<String> NON_DEPENDANT_RELATIONS = ['narrower', 'broader', 'expressionOf', 'related', 'derivedFrom']

    static final Set<String> LD_KEYS

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
    private Map<String, Set<String>> categories
    private Map<String, Set<String>> inRange

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

        categories = generateSubTermLists('category')
        
        def zipMaps = { a, b -> (a.keySet() + b.keySet()).collectEntries{k -> [k, a.get(k, []) + b.get(k, [])]}}
        inRange = zipMaps(generateSubTermLists('rangeIncludes'), generateSubTermLists('range'))
        
        buildLangContainerAliasMap()

        expandAliasesInLensProperties()
        expandInheritedLensProperties()
        expandInverseLensProperties()
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
        def expand = { p ->
            def alias = langContainerAlias[p]
            return alias ? [p, alias] : p
        }
        
        eachLens { lens ->
            lens['showProperties'] = lens['showProperties'].collect { p ->
                isAlternateProperties(p)
                    ? ((Map) p).collectEntries { k, v -> [(k), v.collect{ expand(it) }] }
                    : expand(p)
            }.flatten()
        }
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    expandInheritedLensProperties() {
        def lensesById = [:]
        eachLens { lens ->
            if (lens['@id']) {
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

        eachLens { lens ->
            lens.put('showProperties', flattenedProps(lens))
            lens.remove('fresnel:extends')
        }
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    expandInverseLensProperties() {
        eachLens { lens ->
            lens['inverseProperties'] = ((Iterable) lens['showProperties']).findResults {
                return (it instanceof Map ) && it['inverseOf'] ? it['inverseOf'] : null
            }
        }
    }

    private void eachLens(Closure c) {
        ((Map<String, Map>) displayData['lensGroups'])?.values()?.each { group ->
            ((Map) group.get('lenses'))?.values()?.each { lens ->
                c(lens)
            }
        }
    }

    boolean isSetContainer(String property) {
        context?.get(property)?.with(this.&isSetContainer) ?: false
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

    Set<Link> expandLinks(Set<Link> refs) {
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

    static Set<Link> getExternalReferences(Map jsonLd) {
        Set<Link> allReferences = getAllReferences(jsonLd)
        Set<String> localObjects = getLocalObjects(jsonLd)
        Set<Link> externalRefs = allReferences.findAll { !localObjects.contains(it.getIri()) }
        // NOTE: this is necessary because some documents contain references to
        // bnodes that don't exist (in that document).
        return filterOutDanglingBnodes(externalRefs)
    }

    static Set<Link> expandLinks(Set<Link> refs, Map context) {
        return refs.collect{ it.withIri(expand(it.iri, context)) }.toSet()
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

    private static Set<Link> filterOutDanglingBnodes(Set<Link> refs) {
        return refs.findAll {
            !it.iri.startsWith('_:')
        }
    }

    static Set<Link> getAllReferences(Map jsonLd) {
        if (!jsonLd.containsKey(GRAPH_KEY)) {
            throw new FramingException("Missing '@graph' key in input")
        }
        Set<Link> result = new HashSet<>() 
        DocumentUtil.traverse(jsonLd[GRAPH_KEY]) { value, path ->
            if (value instanceof Map && isReference(value) && !path.contains(JSONLD_ALT_ID_KEY)) {
                def graphIndex = (Integer) path[0]
                List p = path.findAll { !(it instanceof Integer) } // filter out list indices
                if (graphIndex == 0) {
                    p.add(0, RECORD_KEY)
                }
                else if (graphIndex > 1) {
                    p.add(0, graphIndex) // Normally there should only be @graph,0 and @graph,1
                }
                result.add(new Link(relation: p.join('.'), iri: value[ID_KEY]))
            }
            return DocumentUtil.NOP
        }
        return result
    }
    
    private static boolean isReference(Map map) {
        if(map.get(ID_KEY) && map.size() == 1) {
            return true
        } else {
            return false
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

    @TypeChecked(TypeCheckingMode.SKIP)
    void applyInverses(Map thing) {
        thing[REVERSE_KEY]?.each { rel, subjects ->
            Map relDescription = vocabIndex[rel]
            // NOTE: resilient in case we add inverseOf as a direct term
            def inverseOf = relDescription['owl:inverseOf'] ?: relDescription.inverseOf
            List revIds = asList(inverseOf)?.collect {
                toTermKey((String) it[ID_KEY])
            }
            String rev = revIds.find { it in vocabIndex }
            if (rev) {
                asList(thing.get(rev, [])).addAll(subjects)
            }
        }
    }

    static List asList(o) {
        return (o instanceof List) ? (List) o : o != null ? [o] : []
    }
    
    static boolean looksLikeIri(String s) {
        s && (s.startsWith('https://') || s.startsWith('http://'))
    }

    static List<List<String>> findPaths(Map obj, String key, String value) {
        return findPaths(obj, key, [value].toSet())
    }

    static List<List<String>> findPaths(Map obj, String key, Set<String> values) {
        List paths = []
        new DFS().search(obj, { List path, v ->
            if (v in values && key == path[-1]) {
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
                path << n.v2
                node(n.v1)
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
        Map<String, List<String>> superTermOf = [:]
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
        if (!type)
            return false
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
    
    Set<String> getCategoryMembers(String category) {
        return categories.get(category, Collections.EMPTY_SET)
    }

    Set<String> getInRange(String type) {
        return inRange.get(type, Collections.EMPTY_SET)
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

    Map embellish(Map jsonLd, Iterable additionalObjects) {
        if (!jsonLd.get(GRAPH_KEY)) {
            return jsonLd
        }

        List graphItems = jsonLd.get(GRAPH_KEY)

        additionalObjects.each { object ->
            if (object instanceof Map) {
                if (((Map)object).containsKey('@graph')) {
                    graphItems << object
                } else {
                    graphItems << ['@graph': object]
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
            final boolean reduceKey = false, List<List> preservePaths = [], boolean searchCard = false) {
        Map result = [:]

        Map card = removeProperties(thing, getLens(thing, searchCard ? ['search-cards', 'cards'] : ['cards']))
        // If result is too small, use chip instead.
        // TODO: Support and use extends + super in card defs instead.)
        if (card.size() < 2) {
            card = removeProperties(thing, getLens(thing, ['chips']))
        }

        restorePreserved(card, thing, preservePaths)

        // Using a new variable here is because changing the value of reduceKey
        // causes "java.lang.VerifyError: Bad type on operand stack" when running
        // on Java 11. Groovy compiler bug?
        boolean reduce = reduceKey 
                ? reduceKey
                : isSubClassOf((String) thing[TYPE_KEY], 'StructuredValue')
        
        
        card.each { key, value ->
            def lensValue = value
            if (chipsify) {
                lensValue = toChip(value, pathRemainders([key], preservePaths))
            } else {
                if (value instanceof List) {
                    lensValue = ((List) value).withIndex().collect { it, index ->
                        it instanceof Map
                        ? toCard((Map) it, chipsify, addSearchKey, reduce, pathRemainders([key, index], preservePaths), searchCard)
                        : it
                    }
                } else if (value instanceof Map) {
                    lensValue = toCard((Map) value, chipsify, addSearchKey, reduce, pathRemainders([key], preservePaths), searchCard)
                }
            }
            result[key] = lensValue
        }

        if (addSearchKey) {
            List key = makeSearchKeyParts(card)
            
            if (reduce) {
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
            Map reduced = removeProperties(object, getLens(object, ['chips']))
            restorePreserved(reduced, (Map) object, preservePaths)
            reduced.each { key, value ->
                result[key] = toChip(value, pathRemainders([key], preservePaths))
            }
            return result
        } else {
            return object
        }
    }

    /**
     * Applies lens (chip, card, ..) to the thing and returns a list of
     * [<language>, <property value>] pairs.
     */
    private List applyLensAsListByLang(Map thing, Set<String> languagesToKeep, List<String> removableBaseUris, String lensToUse) {
        Map lensGroups = displayData.get('lensGroups')
        Map lensGroup = lensGroups.get(lensToUse)
        Map lens = getLensFor((Map)thing, lensGroup)
        List parts = []

        if (lens) {
            List propertiesToKeep = (List) lens.get('showProperties').findAll({ s -> !(s instanceof String && s.endsWith('ByLang')) })
            // Go through the properties in the order defined in the lens
            for (p in propertiesToKeep) {
                def prop = selectProperty(p, thing, languagesToKeep)
                
                // If prop (e.g., title) has a language-specific version (e.g., titleByLang),
                // and the thing has that language-specific version, use that
                String byLangKey = langContainerAlias.get(prop)
                if (byLangKey && thing[byLangKey] instanceof Map) {
                    languagesToKeep.each { lang ->
                        if (thing[byLangKey][lang]) {
                            parts << [lang, thing[byLangKey][lang]]
                        } else {
                            parts << [lang, thing[prop]]
                        }
                    }
                } else {
                    def values = thing[prop] instanceof List ? thing[prop] : [thing[prop]]
                    values.each { value ->
                        if (value instanceof Map) {
                            if (value.containsKey('@id') && !value.containsKey('@type')) {
                                // e.g. heldBy, ..
                                languagesToKeep.each {
                                    parts << [it, removeDomain((String) value['@id'], removableBaseUris)]
                                }
                            } else {
                                // Check for a more specific lens
                                parts << applyLensAsListByLang((Map) value, languagesToKeep, removableBaseUris, lensToUse)
                            }
                        } else if (value instanceof String) {
                            // Add non-language-specific lens property values
                            languagesToKeep.each { parts << [it, value] }
                        }
                    }
                }
            }
        }

        return parts
    }
    
    private static def selectProperty(def prop, Map thing, Set<String> languagesToKeep) {
        if (!isAlternateProperties(prop)) {
            return prop
        }
        
        def a = prop['alternateProperties'].findResult {
            def hasByLang = { List<String> a -> // List with [prop, propAlias1, propAlias2...]
                 thing[a.head()] || (a.tail().any { 
                     it.endsWith('ByLang') && ((Map<String, ?>) thing.get(it))?.keySet()?.any{ it in languagesToKeep } 
                 })
            }
            if (it instanceof List && hasByLang(it))
            {
                it.first()
            }
            else if (thing[it]) {
                it
            }
        }

        return a ?: prop
    }

    /**
     * Returns a map with the keys given by languagesToKeep, each having as its value a string containing the
       lens property values. For lens 'chip', they would be (approximately) in the order in which they would
       be displayed on the frontend. Mainly for use as search keys.
     */
    Map applyLensAsMapByLang(Map thing, Set<String> languagesToKeep, List<String> removableBaseUris, List<String> lensesToTry) {
        Map lensGroups = displayData.get('lensGroups')
        Map lens = null
        String initialLens

        for (String lensToTry : lensesToTry) {
            Map lensGroup = lensGroups.get(lensToTry)
            lens = getLensFor((Map)thing, lensGroup)
            if (lens) {
                initialLens = lensToTry
                break
            }
        }

        if (!lens) {
            throw new FresnelException("No suitable lens found for ${thing.get(ID_KEY)}, tried: ${lensesToTry}")
        }

        // Transform the list of language/property value pairs to a map
        Map initialResults = languagesToKeep.collectEntries { [(it): []] }
        Map results = applyLensAsListByLang(thing, languagesToKeep, removableBaseUris, initialLens)
            .flatten()
            .collate(2)
            .inject(initialResults, { acc, it ->
                String key = it.get(0)
                if (it.size() == 2 && key in languagesToKeep)
                    ((List) acc[key]) << it.get(1)
                acc
            })

        // Turn the map values into strings
        return results.collectEntries { k, v ->
            String result = ((List) v).findAll { it != null }.flatten().join(", ")
            // Use last URI components as fallback
            if (!result && thing['@id']) {
                result = removeDomain((String) thing['@id'], removableBaseUris)
            } else {
                result = result
                    // Remove leading non-alphanumeric characters.
                    // \p{L} = Lu, Ll, Lt, Lm, Lo; but we don't want Lm as it includes modifier letters like
                    // MODIFIER LETTER PRIME (ʹ) that are sometimes erroneously used.
                    .replaceFirst(/^[^\p{Lu}\p{Ll}\p{Lt}\p{Lo}\p{N}]+/, "")
                    // A string without alphanumerics should not have "" as its sort value, because
                    // then we get messed up records on top when sorting A-Z. Workaround: use a character from
                    // Unicode's Private Use Area, forcing such records to appear at the very end when sorting.
                    // TODO: default to some sensible/explanatory string instead?
                    .replaceFirst(/^$/, "\uE83A")
            }
            [(k): result]
        }
    }

    private static String removeDomain(String uri, List<String> removableBaseUris) {
        for (removableUri in removableBaseUris) {
            if (uri.startsWith(removableUri)) {
                return uri.substring(removableUri.length())
            }
        }
        return uri
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

    Set getInverseProperties(Map data, String lensType) {
        if (data[GRAPH_KEY]) {
            return new LinkedHashSet(((List) data[GRAPH_KEY]).collect{ getInverseProperties((Map) it, lensType) }.flatten())
        }

        Map lensGroups = displayData.get('lensGroups')
        Map lensGroup = lensGroups.get(lensType)
        Map lens = getLensFor(data, lensGroup)
        return new LinkedHashSet((List) lens?.get('inverseProperties') ?: [])
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

    private Map getLens(Map thing, List<String> lensTypes) {
        Map lensGroups = displayData.get('lensGroups')
        lensTypes.findResult { lensType ->
            lensGroups.get(lensType)?.with { getLensFor(thing, (Map) it) }
        }
    }
    
    private static Map removeProperties(Map thing, Map lens) {
        Map result = [:]
        if (lens) {
            List propertiesToKeep = (List) lens.get("showProperties")

            thing.each { key, value ->
                if (shouldAlwaysKeep((String) key)) {
                    result[key] = value
                }
            }
            propertiesToKeep.each { p ->
                if (p instanceof String && thing[p]) {
                    result[p] = thing[p]
                }
                else if (isAlternateProperties(p)) {
                    // We keep _all_ alternate properties, selecting one is more of a display thing 
                    p['alternateProperties'].each { a ->
                        if (a instanceof List) {
                            a.each { if (thing[it]) result[it] = thing[it] }
                        }
                        else if (thing[a]) {
                            result[a] = thing[a]
                        }
                    }
                }
            }
            return result
        } else {
            return thing
        }
    }
    
    private static boolean isAlternateProperties(def p) {
        p instanceof Map && p.size() == 1 && p['alternateProperties']
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

    private static boolean shouldAlwaysKeep(String key) {
        return key == RECORD_KEY || key == THING_KEY || key == JSONLD_ALT_ID_KEY || key.startsWith("@")
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

        Map mainItem = idMap[mainId] as Map

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
            if (key != JSONLD_ALT_ID_KEY)
                newItem.put(key, toEmbedded(value, idMap, embedChain))
            else
                newItem.put(key, value)
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
     * Traverse the data and index all non-reference objects on their @id:s and sameAs:s.
     */
    private static Map getIdMap(Map data) {
        Map idMap = new HashMap()
        populateIdMap(data, idMap)
        return idMap
    }

    private static void populateIdMap(Map data, Map idMap) {
        for (Object key : data.keySet()) {
            if (key == ID_KEY
                // Don't index references (i.e. objects with only an @id).
                && data.keySet().size() > 1
                // Don't index graphs, since their @id:s do not denote them.
                && !data.containsKey(GRAPH_KEY)
               ) {
                addToIdMap(idMap, data, (String) data.get(key))
            } else if (key == JSONLD_ALT_ID_KEY
                    // Don't index graphs, since their @id:s do not denote them.
                    && !data.containsKey(GRAPH_KEY)
                    && data.get(key) instanceof List) {
                List sameAsList = (List) data.get(key)
                for (Object altIdObject : sameAsList) {
                    Map altIdMap = (Map) altIdObject
                    addToIdMap(idMap, data, (String) altIdMap.get(ID_KEY))
                }
            }
            Object obj = data.get(key)
            if (obj instanceof List)
                populateIdMap( (List) obj, idMap )
            else if (obj instanceof Map)
                populateIdMap( (Map) obj, idMap )
        }
    }

    private static void addToIdMap(Map idMap, Map object, String id) {
        // Do not replace a large object with a smaller one (with the same id).
        // Doing so means data is lost in the framing.
        Object preExisting = idMap.get(id)
        if (preExisting != null && preExisting instanceof Map && preExisting.size() > object.size())
            return

        idMap.put(id, object)
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
            String key = map.keySet()[0]
            if (key == ID_KEY) {
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
            super(msg)
        }
    }
}


@Immutable
class Link {
    String iri
    String relation

    Link withIri(String iri) {
        iri == this.iri
                ? this
                : new Link(iri: iri, relation: this.relation)
    }
}


package whelk.datatool.form

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.converter.JsonLdToTrigSerializer
import whelk.datatool.util.DocumentComparator
import whelk.datatool.util.IdLoader
import whelk.util.DocumentUtil

import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.RECORD_TYPE
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.component.SparqlQueryClient.GRAPH_VAR
import static whelk.util.DocumentUtil.getAtPath
import static whelk.util.LegacyIntegrationTools.getMarcCollectionInHierarchy

class Transform {
    private static final DocumentComparator comparator = new DocumentComparator()

    private static final String _ID = '_id'
    private static final String _MATCH = '_match'
    // The following keys are subject to change
    private static final String _ID_LIST = '_idList'
    private static final String VALUE = 'value'
    private static final String VALUE_FROM = 'valueFrom'

    private static final Set<String> IGNORE_CHANGED_VALUE = [_ID_LIST]

    private static final String EMPTY_BLANK_NODE_TMP_ID = "EMPTY_BN_ID"


    Map matchForm
    Map targetForm

    Set<List> exactMatchPaths

    List<List> addedPaths
    List<List> removedPaths

    Map<String, List<String>> nodeIdMappings

    List<ChangesForNode> changes

    static enum MatchingMode {
        EXACT('Exact'),
        SUBSET('Subset')

        String str

        MatchingMode(String str) {
            this.str = str
        }
    }

    Transform(Map matchForm, Map targetForm, Whelk whelk) {
        this.matchForm = matchForm
        this.targetForm = targetForm
        this.removedPaths = collectRemovedPaths()
        this.addedPaths = collectAddedPaths()
        this.exactMatchPaths = collectExactMatchPaths()
        this.nodeIdMappings = collectNodeIdMappings(whelk)
    }

    Transform(Map matchForm, Map targetForm) {
        this(matchForm, targetForm, null)
    }

    Transform() {
    }

    List<Map> getChangeSets() {
        return [
                [
                        (TYPE_KEY)    : 'ChangeSet',
                        'version'     : matchForm,
                        'removedPaths': [],
                        'addedPaths'  : []
                ],
                [
                        (TYPE_KEY)    : 'ChangeSet',
                        'version'     : targetForm,
                        'removedPaths': removedPaths,
                        'addedPaths'  : addedPaths
                ]
        ]
    }

    List<ChangesForNode> getChanges() {
        if (changes == null) {
            changes = (collectRemove() + collectAdd() as List<Change>)
                    .groupBy { it.parentPath() }
                    .collect { parentPath, changeList ->
                        new ChangesForNode(dropIndexes(parentPath),
                                getAtPath(matchForm, parentPath) as Map,
                                changeList,
                                parentPath in exactMatchPaths ? MatchingMode.EXACT : MatchingMode.SUBSET)
                    }
        }
        return changes
    }

    private List<Remove> collectRemove() {
        return (List<Remove>) removedPaths.collect { fullPath ->
            asList(getAtPath(matchForm, fullPath)).collect { value ->
                value instanceof Map
                        ? new Remove(fullPath, value, exactMatchPaths.contains(fullPath) ? MatchingMode.EXACT : MatchingMode.SUBSET)
                        : new Remove(fullPath, value, MatchingMode.EXACT)
            }
        }.flatten()
    }

    private List<Add> collectAdd() {
        return (List<Add>) addedPaths.collect { fullPath ->
            asList(getAtPath(targetForm, fullPath)).collect { value ->
                new Add(fullPath, value instanceof Map ? withoutAnyMarkers(value) : value)
            }
        }.flatten()
    }

    private List<List> collectAddedPaths() {
        return collectChangedPaths(targetForm, matchForm, [])
    }

    private List<List> collectRemovedPaths() {
        return collectChangedPaths(matchForm, targetForm, [])
    }

    private Set<List> collectExactMatchPaths() {
        Set paths = []
        DocumentUtil.findKey(matchForm, _MATCH) { value, path ->
            if (value == MatchingMode.EXACT.str) {
                paths.add(path.dropRight(1))
                return new DocumentUtil.Nop()
            }
        }
        return paths
    }

    private static List collectChangedPaths(Object a, Object b, List path) {
        if (a == b) {
            return []
        }

        if (a && !b) {
            return [path]
        }

        if (a instanceof Map && b instanceof Map) {
            a = withoutIgnored(a)
            b = withoutIgnored(b)

            // Lack of implicit id means that there is a new node at this path
            if (!a[_ID] || !b[_ID]) {
                return [path]
            }
            def changedPaths = []
            a.keySet().each { k ->
                changedPaths.addAll(collectChangedPaths(a[k], b[k], path + k))
            }
            return changedPaths
        }

        if (a instanceof List && b instanceof List) {
            def changedPaths = []
            def sameNode = { x, y ->
                x instanceof Map && y instanceof Map && ((x[_ID] && x[_ID] == y[_ID]) || (x[ID_KEY] && x[ID_KEY] == b[ID_KEY]))
            }
            a.eachWithIndex { aElem, i ->
                def peer = b.find { bElem -> aElem == bElem || sameNode(aElem, bElem) }
                changedPaths.addAll(peer ? collectChangedPaths(aElem, peer, path + i) : [path + i])
            }
            return changedPaths
        }

        if (a instanceof String && b instanceof String) {
            return [path]
        }

        // To be allowed?
        throw new Exception("Changing datatype of a value is not allowed.")
    }

    private static Map withoutIgnored(Map m) {
        if (m.keySet().intersect(IGNORE_CHANGED_VALUE)) {
            m = new HashMap(m)
            m.removeAll { IGNORE_CHANGED_VALUE.contains(it.key) }
        }
        return m
    }

    Iterable<Map> getMatchFormVariants() {
        return getFormVariants(matchForm)
    }

    private Iterable<Map> getFormVariants(Map form) {
        Map bNodeIdToPath = [:]
        DocumentUtil.findKey(form, _ID) { value, path ->
            if (nodeIdMappings.containsKey(value) && value != getThingTmpId() && value != getRecordTmpId()) {
                bNodeIdToPath[value] = path.dropRight(1)
                return new DocumentUtil.Nop()
            }
        }

        Map variant = withoutAnyMarkers(form)

        if (bNodeIdToPath.isEmpty()) {
            return [variant]
        }

        Iterator<Map> i = new Iterator<Map>() {
            private def bNodeIds = bNodeIdToPath.keySet().toList()
            private def idListLengths = bNodeIds.collect { nodeIdMappings[it].size() }
            private def currentIndexes = bNodeIds.collect { 0 }
            private def hasNext = true

            @Override
            boolean hasNext() {
                return hasNext
            }

            @Override
            Map next() {
                // Set ids for current variant
                [bNodeIds, currentIndexes].transpose().each { bNodeId, idx ->
                    def path = bNodeIdToPath[bNodeId]
                    def node = path.isEmpty() ? variant : getAtPath(variant, path)
                    def mappedId = nodeIdMappings[bNodeId][idx]
                    node[ID_KEY] = mappedId
                }

                // Update cursors
                for (i in idListLengths.size() - 1..0) {
                    if (currentIndexes[i] == idListLengths[i] - 1) {
                        currentIndexes[i] = 0
                        if (i == 0) {
                            hasNext = false
                        }
                    } else {
                        currentIndexes[i] += 1
                        break
                    }
                }

                // Return current variant
                return variant
            }
        }

        return () -> i
    }

    Map getMatchFormWithoutAnyMarkers() {
        return withoutAnyMarkers(matchForm)
    }

    Map getMatchFormWithoutNonIdMarkers() {
        return withoutNonIdMarkers(matchForm)
    }

    static Map withoutAnyMarkers(Map form) {
        return withoutMarkers(form, this.&clearAllMarkers)
    }

    static Map withoutNonIdMarkers(Map form) {
        return withoutMarkers(form, this.&clearNonIdMarkers)
    }

    private static withoutMarkers(Map form, Closure clearMarkers) {
        var f = (Map) Document.deepCopy(form)
        clearMarkers(f)
        return f
    }

    private static void clearAllMarkers(Object o) {
        clearMarkers(o, [_ID, _MATCH, _ID_LIST] as Set, [(TYPE_KEY): "Any"])
    }

    private static void clearNonIdMarkers(Object o) {
        clearMarkers(o, [_MATCH, _ID_LIST] as Set, [(TYPE_KEY): "Any"])
    }

    private static void clearMarkers(Object o, Set<String> keys, Map<String, Object> keyValuePairs) {
        DocumentUtil.traverse(o) { v, p ->
            if (v instanceof Map) {
                v.removeAll { keys.contains(it.key) || keyValuePairs[it.key] == it.value }
                return new DocumentUtil.Nop()
            }
        }
    }

    private static List dropLastIndex(List path) {
        return !path.isEmpty() && path.last() instanceof Integer ? path.dropRight(1) : path
    }

    private static List<String> dropIndexes(List path) {
        return path.findAll { it instanceof String } as List<String>
    }

    String getSparqlPattern(Map context) {
        Map form = getMatchFormWithoutNonIdMarkers()

        putIds(form)

        Map thing = form
        Map record = (Map) thing.remove(RECORD_KEY) ?: [:]

        record[ID_KEY] = getRecordTmpId()
        thing[ID_KEY] = getThingTmpId()
        record[THING_KEY] = [(ID_KEY): getThingTmpId()]

        def ttl = ((ByteArrayOutputStream) JsonLdToTrigSerializer.toTurtle(context, [record, thing]))
                .toByteArray()
                .with { new String(it, UTF_8) }
        // Add skip prelude flag to JsonLdToTrigSerializer.toTurtle?
                .with { withoutPrefixes(it) }

        return insertIdMappings(insertVars(ttl))
    }

    private String insertVars(String ttl) {
        def substitutions = [
                ("<" + getThingTmpId() + ">"): getVar(getThingTmpId()),
                ("<" + getRecordTmpId() + ">"): getVar(getRecordTmpId()),
                ("<" + EMPTY_BLANK_NODE_TMP_ID + ">"): "[]",
        ]

        nodeIdMappings.keySet().each {_id ->
            substitutions.put("<" + _id + ">", getVar(_id))
        }

        return ttl.replace(substitutions)
    }

    private String insertIdMappings(String sparqlPattern) {
        def valuesClauses = nodeIdMappings.collect {_id, ids ->
            "VALUES ${getVar(_id)} { ${ ids.collect {"<$it>" }.join(" ") } }\n"
        }.join()
        return valuesClauses + sparqlPattern
    }

    String getVar(String _id) {
        return _id == getRecordTmpId()
                ? "?$GRAPH_VAR"
                : "?${_id.replace('#', '')}"
    }

    private void putIds(Map form) {
        DocumentUtil.traverse(form) { value, path ->
            if (asList(value).isEmpty()) {
                return new DocumentUtil.Replace([(ID_KEY): EMPTY_BLANK_NODE_TMP_ID])
            } else if (value instanceof Map) {
                def _id = value.remove(_ID)
                if (nodeIdMappings.containsKey(_id)) {
                    value[ID_KEY] = _id
                    return new DocumentUtil.Nop()
                } else if (value.isEmpty()) {
                    return new DocumentUtil.Replace([(ID_KEY): EMPTY_BLANK_NODE_TMP_ID])
                }
            }
        }
    }

    private static String withoutPrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith('prefix') }
                .get(1)
                .join('\n')
                .trim()
    }

    Map<String, List<String>> collectNodeIdMappings(Whelk whelk) {
        Map<String, List<String>> nodeIdMappings = [:]

        IdLoader idLoader = whelk ? new IdLoader(whelk.storage) : null

        DocumentUtil.traverse(matchForm) { value, path ->
            if (value instanceof Map && path && dropLastIndex(path).last() == _ID_LIST) {
                def ids = value[VALUE] as List<String>
                        ?: (value[VALUE_FROM] ? idLoader.fromFile((String) value[VALUE_FROM][ID_KEY]) : [])
                if (ids) {
                    def node = getAtPath(matchForm, dropLastIndex(path).dropRight(1))
                    def nodeId = (String) node[_ID]

                    if (!idLoader) {
                        nodeIdMappings[nodeId] = ids
                        return
                    }

                    def (iris, shortIds) = ids.split(JsonLd::looksLikeIri)
                    // Assume given http strings to be valid iris and thus avoid the round trip of first finding the
                    // short id (in lddb__identifiers) and then use the short id only to find (most likely) the same
                    // thing/record iri (in lddb) that we started with.
                    if (shortIds.isEmpty()) {
                        nodeIdMappings[nodeId] = iris
                        return
                    }

                    def nodeType = node[TYPE_KEY]
                    def marcCollection = nodeType ? getMarcCollectionInHierarchy((String) nodeType, whelk.jsonld) : null
                    def xlShortIds = idLoader.collectXlShortIds(shortIds as List<String>, marcCollection)
                    def parentProp = dropIndexes(path).reverse()[1]
                    def isInRange = { type -> whelk.jsonld.getInRange(type).contains(parentProp) }
                    // TODO: Fix hardcoding
                    def isRecord = whelk.jsonld.isInstanceOf((Map) node, "AdminMetadata")
                            || isInRange(RECORD_TYPE)
                            || isInRange("AdminMetadata")
                    nodeIdMappings[(String) node[_ID]] = iris + idLoader.loadAllIds(xlShortIds).collect {
                        isRecord ? it.recordIri() : it.thingIri()
                    }
                    return new DocumentUtil.Nop()
                }
            }
        }

        return nodeIdMappings
    }

    private String getThingTmpId() {
        return matchForm[_ID]
    }

    private String getRecordTmpId() {
        return getAtPath(matchForm, [RECORD_KEY, _ID], "TEMP_ID")
    }

    static boolean isSubset(Object a, Object b) {
        return comparator.isSubset(a, b)
    }

    static boolean isEqual(Object a, Object b) {
        return comparator.isEqual(["x": a], ["x": b], Transform::isEqualNoType)
    }

    private static boolean isEqualNoType(Map a, Map b) {
        if (a == null || b == null) {
            return false
        }
        if (a.size() != b.size()) {
            if (!a.containsKey(TYPE_KEY) && b.containsKey(TYPE_KEY)) {
                b = new HashMap<>(b)
                b.remove(TYPE_KEY)
                return comparator.isEqual(a, b, Transform::isEqualNoType)
            }
            if (a.containsKey(TYPE_KEY) && !b.containsKey(TYPE_KEY)) {
                a = new HashMap<>(a)
                a.remove(TYPE_KEY)
                return comparator.isEqual(a, b, Transform::isEqualNoType)
            }
            return false
        }
        return comparator.isEqual(a, b, Transform::isEqualNoType)
    }

    // Need a better name for this...
    class ChangesForNode {
        List<String> propertyPath
        Map form
        List<Change> changeList
        private MatchingMode matchingMode

        ChangesForNode(List<String> propertyPath, Map form, List<Change> changeList, MatchingMode matchingMode) {
            this.propertyPath = propertyPath
            this.form = form
            this.changeList = changeList
            this.matchingMode = matchingMode
        }

        boolean matches(Map node) {
            return formMatches(node) && removeMatches(node)
        }

        private formMatches(Map node) {
            getFormVariants(form).any {f ->
                switch (matchingMode) {
                    case MatchingMode.EXACT: return isEqual(f, node)
                    case MatchingMode.SUBSET: return isSubset(f, node)
                }
            }
        }

        private removeMatches(Map node) {
            return getRemoveList().every { Remove r -> asList(node[r.property()]).any { r.matches(it) } }
        }

        private List<Remove> getRemoveList() {
            return changeList.findAll { it instanceof Remove } as List<Remove>
        }
    }

    static abstract class Change {
        List path
        Object value

        List<String> propertyPath() {
            return dropIndexes(path)
        }

        List parentPath() {
            return dropLastIndex(path).dropRight(1)
        }

        String property() {
            return propertyPath().last()
        }
    }

    class Remove extends Change {
        private final MatchingMode matchingMode

        Remove(List path, Object value, MatchingMode matchingMode) {
            this.path = path
            this.value = value
            this.matchingMode = matchingMode
        }

        boolean matches(String property, Object o) {
            return property == this.property() && matches(o)
        }

        boolean matches(Object o) {
            if (property() == TYPE_KEY && value == "Any") {
                return true
            }
            return (value instanceof Map ? getFormVariants(value) : [value]).any { v ->
                switch (matchingMode) {
                    case MatchingMode.EXACT: return isEqual(v, o)
                    case MatchingMode.SUBSET: return isSubset(v, o)
                }
            }
        }
    }

    static class Add extends Change {
        // Should matching mode apply to Add too?
        Add(List path, Object value) {
            this.path = path
            this.value = value
        }
    }

    static class MatchForm extends Transform {
        MatchForm(Map matchForm, Whelk whelk) {
            super()
            this.matchForm = matchForm
            this.nodeIdMappings = collectNodeIdMappings(whelk)
        }

        MatchForm(Map matchForm) {
            this(matchForm, null)
        }
    }
}

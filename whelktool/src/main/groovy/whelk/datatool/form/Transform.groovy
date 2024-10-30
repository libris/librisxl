package whelk.datatool.form

import groovy.transform.Memoized
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

    private static final String BNODE_ID = 'bulk:formBlankNodeId'
    private static final String MATCHING_MODE = 'bulk:matchingMode'
    private static final String HAS_ID = 'bulk:hasId'
    private static final String VALUE = 'value'
    private static final String VALUE_FROM = 'bulk:valueFrom'
    private static final String ANY_TYPE = "bulk:Any"
    private static final String SUBTYPES = "bulk:Subtypes"
    private static final String EXACT = 'bulk:Exact'
    private static final String ANY_OF = 'bulk:AnyOf'
    private static final String HAS_BASE_TYPE_TMP = '_hasBaseTypeTmp'

    Map matchForm
    Map targetForm

    List<List> addedPaths
    List<List> removedPaths

    Map<String, List> nodeIdToMatchFormPath
    Map<String, Set<String>> nodeIdMappings
    Map<String, Set<String>> baseTypeMappings

    List<ChangesForNode> changes

    Transform(Map matchForm, Map targetForm, Whelk whelk) {
        this.matchForm = matchForm
        this.targetForm = targetForm
        this.removedPaths = collectRemovedPaths()
        this.addedPaths = collectAddedPaths()
        this.nodeIdToMatchFormPath = collectNodeIdToPath(matchForm)
        this.nodeIdMappings = collectNodeIdMappings(whelk)
        this.baseTypeMappings = collectBaseTypeMappings(whelk?.jsonld)
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
                    .groupBy { it.parentId() }
                    .collect { nodeId, changeList -> new ChangesForNode(nodeId, changeList) }
                    .sort {it.nodeId }
        }
        return changes
    }

    private List<Remove> collectRemove() {
        return (List<Remove>) removedPaths.collect { fullPath ->
            asList(getAtPath(matchForm, fullPath)).collect { value ->
                new Remove(fullPath, value)
            }
        }.flatten()
    }

    private List<Add> collectAdd() {
        return (List<Add>) addedPaths.collect { fullPath ->
            asList(getAtPath(targetForm, fullPath)).collect { value ->
                new Add(fullPath, value)
            }
        }.flatten()
    }

    private List<List> collectAddedPaths() {
        return collectChangedPaths(targetForm, matchForm, [])
    }

    private List<List> collectRemovedPaths() {
        return collectChangedPaths(matchForm, targetForm, [])
    }

    private static List collectChangedPaths(Object a, Object b, List path) {
        if (a == b) {
            return []
        }

        if (a && !b) {
            return [path]
        }

        if (a instanceof Map && b instanceof Map) {
            // Lack of implicit id means that there is a new node at this path
            if (!a[BNODE_ID] || !b[BNODE_ID]) {
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
                x instanceof Map && y instanceof Map && ((x[BNODE_ID] && x[BNODE_ID] == y[BNODE_ID]) || (x[ID_KEY] && x[ID_KEY] == b[ID_KEY]))
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

    private static List dropLastIndex(List path) {
        return !path.isEmpty() && path.last() instanceof Integer ? path.dropRight(1) : path
    }

    private static List<String> dropIndexes(List path) {
        return path.findAll { it instanceof String } as List<String>
    }

    private static Map<String, List> collectNodeIdToPath(Map form) {
        Map<String, List> nodeIdToPath = [:]
        DocumentUtil.findKey(form, BNODE_ID) { nodeId, path ->
            nodeIdToPath[(String) nodeId] = path.dropRight(1)
            return new DocumentUtil.Nop()
        }
        return nodeIdToPath
    }

    String getSparqlPattern(Map context) {
        Map thing = getSparqlPreparedForm()
        Map record = (Map) thing.remove(RECORD_KEY) ?: [:]

        record[ID_KEY] = getRecordTmpId()
        thing[ID_KEY] = getThingTmpId()
        record[THING_KEY] = [(ID_KEY): getThingTmpId()]

        def ttl = ((ByteArrayOutputStream) JsonLdToTrigSerializer.toTurtle(context, [record, thing]))
                .toByteArray()
                .with { new String(it, UTF_8) }
        // Add skip prelude flag to JsonLdToTrigSerializer.toTurtle?
                .with { withoutPrefixes(it) }

        return insertTypeMappings(insertIdMappings(insertVars(ttl)))
    }

    private Map getSparqlPreparedForm() {
        Map matchFormCopy = (Map) Document.deepCopy(matchForm)

        DocumentUtil.traverse(matchFormCopy) { node, path ->
            if (node instanceof Map) {
                def bNodeId = node.remove(BNODE_ID)
                if (!bNodeId) return
                node.remove(HAS_ID)
                if (node[TYPE_KEY] == ANY_TYPE) {
                    node.remove(TYPE_KEY)
                }
                if (asList(node.remove(MATCHING_MODE)).contains(SUBTYPES)) {
                    def baseType = node.remove(TYPE_KEY)
                    node[HAS_BASE_TYPE_TMP] = baseType
                }
                if (nodeIdMappings.containsKey(bNodeId)) {
                    node[ID_KEY] = bNodeId
                }
                return new DocumentUtil.Nop()
            }
            if (asList(node).isEmpty()) {
                return new DocumentUtil.Replace([:])
            }
        }

        return matchFormCopy
    }

    private String insertVars(String ttl) {
        def substitutions = [
                ("<" + getThingTmpId() + ">") : getVar(getThingTmpId()),
                ("<" + getRecordTmpId() + ">"): getVar(getRecordTmpId())
        ]

        baseTypeMappings.keySet().each { baseType ->
            substitutions.put(":$HAS_BASE_TYPE_TMP \"$baseType\"".toString(), "a ?" + baseType)
        }

        nodeIdMappings.keySet().each { _id ->
            substitutions.put("<" + _id + ">", getVar(_id))
        }

        return ttl.replace(substitutions)
    }

    private String insertTypeMappings(String sparqlPattern) {
        def valuesClause = baseTypeMappings.collect { baseType, subTypes ->
            "VALUES ?$baseType { ${([baseType] + subTypes).collect { ":$it" }.join(" ")} }\n"
        }.join()
        return valuesClause + sparqlPattern
    }

    private String insertIdMappings(String sparqlPattern) {
        def valuesClauses = nodeIdMappings.collect { _id, ids ->
            "VALUES ${getVar(_id)} { ${ids.collect { "<$it>" }.join(" ")} }\n"
        }.join()
        return valuesClauses + sparqlPattern
    }

    String getVar(String bNodeId) {
        return bNodeId == getRecordTmpId()
                ? "?$GRAPH_VAR"
                : "?${bNodeId.replace('#', '')}"
    }

    private static String withoutPrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith('prefix') }
                .get(1)
                .join('\n')
                .trim()
    }

    Map<String, Set<String>> collectNodeIdMappings(Whelk whelk) {
        Map<String, Set<String>> nodeIdMappings = [:]

        IdLoader idLoader = whelk ? new IdLoader(whelk.storage) : null

        DocumentUtil.traverse(matchForm) { node, path ->
            if (!(node instanceof Map)) {
                return
            }
            def anyOf = asList(node[HAS_ID]).find { it[TYPE_KEY] == ANY_OF }
            if (!anyOf) {
                return
            }
            def ids = (anyOf[VALUE] ?: (anyOf[VALUE_FROM] ? IdLoader.fromFile((String) anyOf[VALUE_FROM][ID_KEY]) : [])) as Set<String>
            if (ids) {
                String nodeId = node[BNODE_ID]

                def (iris, shortIds) = ids.split(JsonLd::looksLikeIri)
                if (shortIds.isEmpty()) {
                    nodeIdMappings[nodeId] = iris
                    return
                }

                if (!idLoader) {
                    nodeIdMappings[nodeId] = iris + shortIds.collect { Document.BASE_URI.toString() + it + Document.HASH_IT }
                    return
                }

                def nodeType = node[TYPE_KEY]
                def marcCollection = nodeType ? getMarcCollectionInHierarchy((String) nodeType, whelk.jsonld) : null
                def xlShortIds = idLoader.collectXlShortIds(shortIds as List<String>, marcCollection)
                def parentProp = dropIndexes(path).reverse()[1]
                def isInRange = { type -> whelk.jsonld.getInRange(type).contains(parentProp) }
                // TODO: Fix hardcoding
                def isRecord = whelk.jsonld.isInstanceOf(node, "AdminMetadata")
                        || isInRange(RECORD_TYPE)
                        || isInRange("AdminMetadata")

                nodeIdMappings[nodeId] = iris + xlShortIds.collect {
                    Document.BASE_URI.toString() + it + (isRecord ? "" : Document.HASH_IT)
                }

                return new DocumentUtil.Nop()
            }
        }

        return nodeIdMappings
    }

    Map<String, Set<String>> collectBaseTypeMappings(JsonLd jsonLd) {
        Map<String, Set<String>> mappings = [:]

        if (jsonLd == null) {
            return mappings
        }

        DocumentUtil.traverse(matchForm) { node, path ->
            if (node instanceof Map && node.containsKey(MATCHING_MODE) && ((List) node[MATCHING_MODE]).contains(SUBTYPES)) {
                def baseType = (String) node[TYPE_KEY]
                Set<String> subTypes = getSubtypes(baseType, jsonLd) as Set
                mappings[baseType] = subTypes
                return new DocumentUtil.Nop()
            }
        }

        return mappings
    }

    @Memoized
    private static Set<String> getSubtypes(String type, JsonLd jsonLd) {
        return jsonLd.getSubClasses(type)
    }

    private String getThingTmpId() {
        return matchForm[BNODE_ID]
    }

    private String getRecordTmpId() {
        return getAtPath(matchForm, [RECORD_KEY, BNODE_ID], "TEMP_ID")
    }

    boolean matches(Object node) {
        return matches(matchForm, node)
    }

    boolean matches(Object matchForm, Object node) {
        return comparator.isSubset(["x": matchForm], ["x": node], this::_matches)
    }

    private boolean _matches(Map matchForm, Map bNode) {
        if (matchForm == null || bNode == null) {
            return false
        }
        matchForm = new LinkedHashMap(matchForm)
        def match = asList(matchForm[MATCHING_MODE])
        if (match.contains(EXACT)) {
            return exactMatches(matchForm, bNode)
        }
        if (match.contains(SUBTYPES)) {
            String aType = matchForm[TYPE_KEY]
            String bType = bNode[TYPE_KEY]
            if (!(baseTypeMappings[aType] + aType).contains(bType)) {
                return false
            } else {
                matchForm.remove(TYPE_KEY)
            }
        }
        matchForm.remove(MATCHING_MODE)
        if (matchForm[TYPE_KEY] == ANY_TYPE) {
            matchForm.remove(TYPE_KEY)
        }
        def ids = nodeIdMappings[matchForm.remove(BNODE_ID)]
        if (ids && !ids.contains(bNode[ID_KEY])) {
            return false
        }
        matchForm.remove(HAS_ID)
        if (matchForm.size() > bNode.size()) {
            return false
        }
        return comparator.isSubset(matchForm, bNode, this::_matches)
    }

    private boolean exactMatches(Map matchForm, Map bNode) {
        if (matchForm == null || bNode == null) {
            return false
        }
        matchForm = new HashMap(matchForm)
        bNode = new HashMap(bNode)
        if (asList(matchForm.remove(MATCHING_MODE)).contains(SUBTYPES)) {
            String aType = matchForm[TYPE_KEY]
            String bType = bNode[TYPE_KEY]
            if ((baseTypeMappings[aType] + aType).contains(bType)) {
                matchForm.remove(TYPE_KEY)
                bNode.remove(TYPE_KEY)
            } else {
                return false
            }
        }
        if (matchForm[TYPE_KEY] == ANY_TYPE) {
            matchForm.remove(TYPE_KEY)
            bNode.remove(TYPE_KEY)
        }
        def ids = nodeIdMappings[matchForm.remove(BNODE_ID)]
        if (ids && !ids.contains(bNode[ID_KEY])) {
            return false
        }
        matchForm.remove(HAS_ID)
        if (matchForm.size() != bNode.size()) {
            return false
        }
        return comparator.isEqual(matchForm, bNode, this::exactMatches)
    }

    Add newAddValue(Object value) {
        return new Add(null, value)
    }

    // Need a better name for this...
    class ChangesForNode {
        String nodeId
        List<Change> changeList
        List<String> propertyPath

        ChangesForNode(String nodeId, List<Change> changeList) {
            this.nodeId = nodeId
            this.changeList = changeList
            this.propertyPath = dropIndexes(nodeIdToMatchFormPath[nodeId])
        }

        boolean matches(Map node) {
            return matches(form(), node) && removeMatches(node)
        }

        private removeMatches(Map node) {
            return getRemoveList().every { Remove r -> asList(node[r.property()]).any { r.matches(it) } }
        }

        private List<Remove> getRemoveList() {
            return changeList.findAll { it instanceof Remove } as List<Remove>
        }

        boolean shouldMatchExact() {
            return asList(form()[MATCHING_MODE]).contains(EXACT)
        }

        boolean matchAnyType() {
            form()[TYPE_KEY] == ANY_TYPE
        }

        private Map form() {
            return getAtPath(matchForm, nodeIdToMatchFormPath[nodeId]) as Map
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

        boolean shouldMatchExact() {
            return value instanceof Map && asList(value[MATCHING_MODE]).contains(EXACT)
        }

        boolean matchAnyType() {
            value instanceof Map && value[TYPE_KEY] == ANY_TYPE
        }

        abstract boolean matches(Object o)

        abstract String parentId()
    }

    class Remove extends Change {
        Remove(List path, Object value) {
            this.path = path
            this.value = value
        }

        boolean matches(String property, Object o) {
            return property == this.property() && matches(o)
        }

        boolean matches(Object o) {
            return Transform.this.matches(value, o) || (property() == TYPE_KEY && value == ANY_TYPE)
        }

        String parentId() {
            getAtPath(matchForm, parentPath())[BNODE_ID]
        }

        boolean hasId() {
            value instanceof Map && value.containsKey(HAS_ID)
        }
    }

    class Add extends Change {
        // Should matching mode apply to Add too?
        Add(List path, Object value) {
            this.path = path
            this.value = value
        }

        boolean matches(Object o) {
            return comparator.isEqual(["x": value], ["x": o])
        }

        String parentId() {
            getAtPath(targetForm, parentPath())[BNODE_ID]
        }
    }

    static class MatchForm extends Transform {
        MatchForm(Map matchForm, Whelk whelk) {
            super()
            this.matchForm = matchForm
            this.nodeIdMappings = collectNodeIdMappings(whelk)
            this.baseTypeMappings = collectBaseTypeMappings(whelk?.jsonld)
        }

        MatchForm(Map matchForm) {
            this(matchForm, null)
        }
    }
}

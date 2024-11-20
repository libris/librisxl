package whelk.datatool.form

import groovy.transform.Memoized
import groovy.transform.PackageScope
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.datatool.util.DocumentComparator
import whelk.datatool.util.IdLoader
import whelk.util.DocumentUtil

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.RECORD_TYPE
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.component.SparqlQueryClient.GRAPH_VAR
import static whelk.converter.JsonLDTurtleConverter.toTurtleNoPrelude
import static whelk.util.DocumentUtil.getAtPath
import static whelk.util.LegacyIntegrationTools.getMarcCollectionInHierarchy

class MatchForm {
    private static final DocumentComparator comparator = new DocumentComparator()

    public static final String MATCHING_MODE = 'bulk:matchingMode'
    public static final String HAS_ID = 'bulk:hasId'
    public static final String BNODE_ID = 'bulk:formBlankNodeId'
    public static final String ANY_TYPE = "bulk:Any"
    public static final String SUBTYPES = "bulk:Subtypes"
    public static final String EXACT = 'bulk:Exact'

    private static final String VALUE = 'value'
    private static final String VALUE_FROM = 'bulk:valueFrom'
    private static final String ANY_OF = 'bulk:AnyOf'
    private static final String HAS_BASE_TYPE_TMP = '_hasBaseTypeTmp'

    Map form

    // For looking up where in the form a certain blank node is located
    Map<String, List> formBNodeIdToPath
    // For looking up resource ids (if given in bulk:hasId) associated with a certain blank node in the form
    Map<String, Set<String>> formBNodeIdToResourceIds
    // For looking up subtypes of a type appearing in the form
    Map<String, Set<String>> baseTypeToSubtypes

    MatchForm(Map form, Whelk whelk) {
        this.form = form
        this.formBNodeIdToPath = collectFormBNodeIdToPath()
        this.formBNodeIdToResourceIds = collectFormBNodeIdToResourceIds(whelk)
        this.baseTypeToSubtypes = collectBaseTypeToSubtypes(whelk?.jsonld)
    }

    // For testing only
    @PackageScope
    MatchForm(Map matchForm) {
        this(matchForm, null)
    }

    // For testing only
    @PackageScope
    MatchForm() {}

    boolean matches(Object node) {
        return matches(form, node)
    }

    boolean matches(Object matchForm, Object node) {
        return comparator.isSubset(["x": matchForm], ["x": node], this::mapMatches)
    }

    String getSparqlPattern(Map context) {
        Map thing = getSparqlPreparedForm()
        Map record = (Map) thing.remove(RECORD_KEY) ?: [:]

        record[ID_KEY] = getRecordTmpId()
        thing[ID_KEY] = getThingTmpId()
        record[THING_KEY] = [(ID_KEY): getThingTmpId()]

        Map graph = [(GRAPH_KEY): [record, thing]]

        String ttl = toTurtleNoPrelude(graph, context)

        return insertTypeMappings(insertIdMappings(insertVars(ttl)))
    }

    static List<String> dropIndexes(List path) {
        return path.findAll { it instanceof String } as List<String>
    }

    private boolean mapMatches(Map matchForm, Map bNode) {
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
            if (!(baseTypeToSubtypes[aType] + aType).contains(bType)) {
                return false
            } else {
                matchForm.remove(TYPE_KEY)
            }
        }
        matchForm.remove(MATCHING_MODE)
        if (matchForm[TYPE_KEY] == ANY_TYPE) {
            matchForm.remove(TYPE_KEY)
        }
        def ids = formBNodeIdToResourceIds[matchForm.remove(BNODE_ID)]
        if (ids && !ids.contains(bNode[ID_KEY])) {
            return false
        }
        matchForm.remove(HAS_ID)
        if (matchForm.size() > bNode.size()) {
            return false
        }
        return comparator.isSubset(matchForm, bNode, this::mapMatches)
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
            if ((baseTypeToSubtypes[aType] + aType).contains(bType)) {
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
        def ids = formBNodeIdToResourceIds[matchForm.remove(BNODE_ID)]
        if (ids && !ids.contains(bNode[ID_KEY])) {
            return false
        }
        matchForm.remove(HAS_ID)
        if (matchForm.size() != bNode.size()) {
            return false
        }
        return comparator.isEqual(matchForm, bNode, this::exactMatches)
    }

    private Map getSparqlPreparedForm() {
        Map matchFormCopy = (Map) Document.deepCopy(form)

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
                if (formBNodeIdToResourceIds.containsKey(bNodeId)) {
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

        baseTypeToSubtypes.keySet().each { baseType ->
            substitutions.put(":$HAS_BASE_TYPE_TMP \"$baseType\"".toString(), "a ?" + baseType)
        }

        formBNodeIdToResourceIds.keySet().each { _id ->
            substitutions.put("<" + _id + ">", getVar(_id))
        }

        return ttl.replace(substitutions)
    }


    private String insertTypeMappings(String sparqlPattern) {
        def valuesClause = baseTypeToSubtypes.collect { baseType, subTypes ->
            "VALUES ?$baseType { ${([baseType] + subTypes).collect { ":$it" }.join(" ")} }\n"
        }.join()
        return valuesClause + sparqlPattern
    }

    private String insertIdMappings(String sparqlPattern) {
        def valuesClauses = formBNodeIdToResourceIds.collect { _id, ids ->
            "VALUES ${getVar(_id)} { ${ids.collect { "<$it>" }.join(" ")} }\n"
        }.join()
        return valuesClauses + sparqlPattern
    }

    private String getVar(String bNodeId) {
        return bNodeId == getRecordTmpId()
                ? "?$GRAPH_VAR"
                : "?${bNodeId.replace('#', '')}"
    }

    private String getThingTmpId() {
        return form[BNODE_ID]
    }

    private String getRecordTmpId() {
        return getAtPath(form, [RECORD_KEY, BNODE_ID], "TEMP_ID")
    }

    private Map<String, Set<String>> collectFormBNodeIdToResourceIds(Whelk whelk) {
        return collectFormBNodeIdToResourceIds(form, whelk)
    }

    private static Map<String, Set<String>> collectFormBNodeIdToResourceIds(Map form, Whelk whelk) {
        Map<String, Set<String>> nodeIdMappings = [:]

        IdLoader idLoader = whelk ? new IdLoader(whelk.storage) : null

        DocumentUtil.traverse(form) { node, path ->
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

    private Map<String, List> collectFormBNodeIdToPath() {
        Map<String, List> nodeIdToPath = [:]
        DocumentUtil.findKey(form, BNODE_ID) { nodeId, path ->
            nodeIdToPath[(String) nodeId] = path.dropRight(1)
            return new DocumentUtil.Nop()
        }
        return nodeIdToPath
    }

    private Map<String, Set<String>> collectBaseTypeToSubtypes(JsonLd jsonLd) {
        Map<String, Set<String>> mappings = [:]

        if (jsonLd == null) {
            return mappings
        }

        DocumentUtil.traverse(form) { node, path ->
            if (node instanceof Map && node.containsKey(MATCHING_MODE) && ((List) node[MATCHING_MODE]).contains(SUBTYPES)) {
                def baseType = (String) node[TYPE_KEY]
                Set<String> subTypes = getSubtypes(baseType, jsonLd) as Set
                mappings[baseType] = subTypes
                return new DocumentUtil.Nop()
            }
        }

        return mappings
    }

    private static Set<String> getSubtypes(String type, JsonLd jsonLd) {
        return jsonLd.getSubClasses(type)
    }
}

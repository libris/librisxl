package whelk.datatool.form

import groovy.transform.PackageScope
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.datatool.util.DocumentComparator
import whelk.datatool.util.IdLoader
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.Rdfs.RANGE
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.RECORD_TYPE
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.JsonLd.looksLikeIri
import static whelk.component.SparqlQueryClient.GRAPH_VAR
import static whelk.converter.JsonLDTurtleConverter.toTurtleNoPrelude
import static whelk.util.DocumentUtil.getAtPath
import static whelk.util.LegacyIntegrationTools.getMarcCollectionInHierarchy
import static whelk.datatool.util.IdLoader.Id

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

    Map form

    // For looking up where in the form a certain blank node is located
    Map<String, List> formBNodeIdToPath
    // For looking up resource ids (if given in bulk:hasId) associated with a certain blank node in the form
    Map<String, Map<String, Id>> formBNodeIdToResourceIds
    // For looking up subtypes of a type appearing in the form
    Map<String, Set<String>> baseTypeToSubtypes

    MatchForm(Map form, Whelk whelk) {
        this.form = form
        this.formBNodeIdToPath = collectFormBNodeIdToPath(form)
        this.formBNodeIdToResourceIds = collectFormBNodeIdToResourceIds(form, whelk)
        this.baseTypeToSubtypes = collectBaseTypeToSubtypes(whelk?.jsonld)
    }

    MatchForm(Map form) {
        this(form, null)
    }

    boolean matches(Map thing) {
        return matches(form, thing)
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

        String ttl = toTurtleNoPrelude([record, thing], context)

        return insertTypeMappings(insertVars(ttl))
    }

    // If there is an ID list associated with the top level node, then that specifies the record selection.
    List<String> getIdSelection() {
        return formBNodeIdToResourceIds[getThingTmpId()]?.values().collect { it.shortId() }
    }

    // There can be multiple ID lists, each associated with a different path.
    // This specifies the selection by saying that every record must, at each path, link to any of the associated IDs.
    Map<String, List<String>> getIdListsForPaths() {
        return formBNodeIdToResourceIds.collectEntries{ bNodeId, idMap ->
            [dropIndexes(formBNodeIdToPath[bNodeId]).join("."), idMap.values().collect { it.shortId() }]
        }
    }

    static List<String> dropIndexes(List path) {
        return path.findAll { it instanceof String } as List<String>
    }

    private Set<String> getSubtypes() {
        return getSubtypes(form)
    }

    private Set<String> getSubtypes(Map formBNode) {
        return baseTypeToSubtypes[formBNode[TYPE_KEY]]
    }

    private boolean shouldMatchSubtypes() {
        return shouldMatchSubtypes(form)
    }

    private static boolean shouldMatchSubtypes(Map formBNode) {
        return asList(formBNode[MATCHING_MODE]).contains(SUBTYPES)
    }

    private static boolean shouldMatchExact(Map formBNode) {
        return asList(formBNode[MATCHING_MODE]).contains(EXACT)
    }

    private boolean mapMatches(Map formBNode, Map bNode) {
        if (formBNode == null || bNode == null) {
            return false
        }

        formBNode = new LinkedHashMap(formBNode)

        if (shouldMatchExact(formBNode)) {
            return exactMatches(formBNode, bNode)
        }
        if (formBNode[TYPE_KEY] && !typeMatches(formBNode, bNode)) {
            return false
        }
        formBNode.remove(TYPE_KEY)
        formBNode.remove(MATCHING_MODE)

        if (!idMatches(formBNode, bNode)) {
            return false
        }
        formBNode.remove(HAS_ID)
        formBNode.remove(BNODE_ID)

        if (formBNode.size() > bNode.size()) {
            return false
        }

        return comparator.isSubset(formBNode, bNode, this::mapMatches)
    }

    private boolean exactMatches(Map formBNode, Map bNode) {
        if (formBNode == null || bNode == null) {
            return false
        }

        formBNode = new HashMap(formBNode)
        bNode = new HashMap(bNode)

        if (!typeMatches(formBNode, bNode)) {
            return false
        }
        formBNode.remove(TYPE_KEY)
        bNode.remove(TYPE_KEY)
        formBNode.remove(MATCHING_MODE)

        if (!idMatches(formBNode, bNode)) {
            return false
        }
        formBNode.remove(HAS_ID)
        formBNode.remove(BNODE_ID)

        if (formBNode.size() != bNode.size()) {
            return false
        }

        return comparator.isEqual(formBNode, bNode, this::exactMatches)
    }

    private boolean idMatches(Map formBNode, Map bNode) {
        def ids = formBNodeIdToResourceIds[formBNode[BNODE_ID]]?.keySet()
        return ids ? ids.contains(bNode[ID_KEY]) : true
    }

    private boolean typeMatches(Map formBNode, Map bNode) {
        return (formBNode[TYPE_KEY] == ANY_TYPE || formBNode[TYPE_KEY] == bNode[TYPE_KEY])
                || (shouldMatchSubtypes(formBNode) && hasSameBaseType(formBNode, bNode))
    }

    private boolean hasSameBaseType(Map formBNode, Map bNode) {
        ([formBNode[TYPE_KEY]] + getSubtypes(formBNode)).contains(bNode[TYPE_KEY])
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
                node.remove(MATCHING_MODE)
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

        def sparqlPattern = ttl.replace(substitutions)

        if (shouldMatchSubtypes() && getSubtypes()) {
            def baseType = form[TYPE_KEY]
            def thingVar = getVar(getThingTmpId())
            return sparqlPattern.replace("$thingVar a :$baseType", "$thingVar a ?$baseType")
        }

        return sparqlPattern
    }


    private String insertTypeMappings(String sparqlPattern) {
        if (shouldMatchSubtypes() && getSubtypes()) {
            String baseType = form[TYPE_KEY]
            String values = ([baseType] + getSubtypes()).collect { it.contains(":") ? it : ":$it" }.join(" ")
            String valuesClause = "VALUES ?$baseType { $values }\n"
            return valuesClause + sparqlPattern
        }
        return sparqlPattern
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

    static Map<String, Map<String, Id>> collectFormBNodeIdToResourceIds(Map form, Whelk whelk) {
        Map<String, Map<String, Id>> nodeIdMappings = [:]

        def getAnyOf = { it instanceof Map ? asList(it[HAS_ID]).find { it[TYPE_KEY] == ANY_OF } : null }

        if (whelk == null) {
            // For unit testing without a Whelk instance at hand
            DocumentUtil.traverse(form) { node, path ->
                def ids = getAnyOf(node)?[VALUE] as List<String>
                if (ids) {
                    String nodeId = node[BNODE_ID]
                    nodeIdMappings[nodeId] = ids.collectEntries { [it, null] } as Map<String, Id>
                    return new DocumentUtil.Nop()
                }
            }
            return nodeIdMappings
        }

        IdLoader idLoader = new IdLoader(whelk.storage)

        DocumentUtil.traverse(form) { node, path ->
            def anyOf = getAnyOf(node)
            if (!anyOf) {
                return
            }
            String nodeId = node[BNODE_ID]
            def ids = (anyOf[VALUE] ?: (anyOf[VALUE_FROM] ? IdLoader.fromFile((String) anyOf[VALUE_FROM][ID_KEY]) : [])) as List<String>
            if (ids) {
                def parentProp = dropIndexes(path).reverse()[0]
                def nodeType = node[TYPE_KEY] ?: getUnambiguousRange(parentProp, whelk.jsonld)
                def marcCollection = nodeType ? getMarcCollectionInHierarchy((String) nodeType, whelk.jsonld) : null
                def xlShortIds = idLoader.collectXlShortIds(ids, marcCollection)
                def isInRange = { type -> whelk.jsonld.getInRange(type).contains(parentProp) }
                // TODO: Fix hardcoding
                def isRecord = whelk.jsonld.isInstanceOf((Map) node, "AdminMetadata")
                        || isInRange(RECORD_TYPE)
                        || isInRange("AdminMetadata")

                nodeIdMappings[nodeId] = idLoader.loadAllIds(xlShortIds)
                        .collectEntries {
                            // The key here is the IRI as it appears in the data
                            [isRecord ? it.recordIri() : it.thingIri(), it]
                        } as Map<String, Id>

                return new DocumentUtil.Nop()
            }
        }

        return nodeIdMappings
    }

    static Map<String, List> collectFormBNodeIdToPath(Map form) {
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
            if (node instanceof Map && shouldMatchSubtypes(node)) {
                def baseType = (String) node[TYPE_KEY]
                mappings[baseType] = jsonLd.getSubClasses(baseType)
                return new DocumentUtil.Nop()
            }
        }

        return mappings
    }

    private static String getUnambiguousRange(String property, JsonLd jsonLd) {
        List range = getAtPath(jsonLd.vocabIndex, [property, RANGE, "*", ID_KEY], [])
        return range.size() == 1 ? jsonLd.toTermKey((String) range.first()) : null
    }
}

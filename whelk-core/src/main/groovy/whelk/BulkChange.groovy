package whelk

import whelk.converter.JsonLdToTrigSerializer
import whelk.util.DocumentUtil
import whelk.util.DocumentComparator
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet

import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.THING_KEY

import static whelk.JsonLd.asList

class BulkChange {
    private static final String FORM = 'form'
    private static final String OPERATIONS = 'operations'
    private static final String PROPERTY = 'property'
    private static final String DELETE = 'delete'
    private static final String INSERT = 'insert'
    private static final String MAPPINGS = 'mappings'
    private static final String PATH = 'path'

    private static final String GRAPH_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"
    private static final String MAPPINGS_PLACEHOLDER = "MAPPINGS_PLACEHOLDER"

    private static final String GRAPH = 'graph'

    private static final int LIMIT = 3

    Whelk whelk
    Map spec
    DocumentComparator comparator

    List<String> ids
    List<Map> data
    List<Map> modifiedData

    BulkChange(Whelk whelk, Map spec, List<String> ids = [], boolean deleteRecords = false) {
        this.spec = spec
        this.whelk = whelk
        this.comparator = new DocumentComparator()
        this.ids = loadIds(ids, spec[FORM], whelk.jsonld.getContext())
        if (deleteRecords) {
            deleteSelection(whelk, this.ids)
        } else {
            this.data = loadData(whelk, this.ids)
            this.modifiedData = modify(data, spec, this.comparator, whelk.jsonld.getRepeatableTerms())
        }
    }

    static List<String> loadIds(List<String> inputIds, Map form, Map context) {
        if (form) {
            def queriedIds = sparqlQueryIds(form, context)
            return inputIds ? queriedIds.intersect(inputIds) : queriedIds
        }
        return inputIds
    }

    static void deleteSelection(Whelk whelk, List<String> ids) {
        ids.each { id ->
            // TODO: Proper parameters
            whelk.remove(id, 'xl', "TODO: Job URI")
        }
    }

    static List<Map> loadData(Whelk whelk, List<String> ids) {
        return whelk.bulkLoad(ids).collect { id, doc ->
            def (record, thing) = doc.data[GRAPH_KEY]
            thing[RECORD_KEY] = record
            doc.data = thing
            return thing
        }
    }

    static List<String> sparqlQueryIds(Map form, Map context, Map mappings = null) {
        def ttl = toTurtle(form, context)
        def (prefixes, ttlGraph) = separatePrefixes(ttl)
        def graphPattern = sparqlify(ttlGraph)
        def valuesClause = ""
        def altPattern = ""
        if (mappings) {
            valuesClause = "VALUES ?${MAPPINGS} { " + mappings.collect { from, to -> "\"$from\"" }.join(' ') + " }"
            altPattern = graphPattern
                    ? graphPattern.replaceFirst(~/"$MAPPINGS_PLACEHOLDER"/, "?${MAPPINGS}")
                    : "?s ?p ?${MAPPINGS}"
        }

        def queryString = """
            ${prefixes}

            SELECT DISTINCT ?${GRAPH} WHERE {
                ${valuesClause}

                ${GRAPH} ?${GRAPH} {
                    ${altPattern ?: graphPattern}
                }
            }
        """

        if (LIMIT > 0) {
            queryString += "\nLIMIT ${LIMIT}"
        }

        //TODO: Get sparql endpoint for environment properly (not via document base uri)
        def endpoint = Document.BASE_URI.toString() + "sparql"
        QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, queryString)
        ResultSet res = qe.execSelect()
        return res.collect { it.get(GRAPH).toString() }
    }

    static List<String> separatePrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith("prefix") }
                .collect { it.join('\n') }
    }

    static String toTurtle(Map thing, Map context = null) {
        def atGraphForm = [thing.remove(RECORD_KEY), thing]
        def bytes = JsonLdToTrigSerializer.toTurtle(context, atGraphForm).toByteArray()
        return new String(bytes, UTF_8)
    }

    static String sparqlify(String ttl) {
        def substitutions = [
                "<$GRAPH_TMP_ID>": "?$GRAPH",
                "<$THING_TMP_ID>": "?mainEntity",
        ]
        return ttl.replace(substitutions)
    }

    static modify(List<Map> data, Map spec, DocumentComparator c, Set<String> repeatableTerms) {
        def updated = []
        def globalMappings = spec[MAPPINGS]
        def form = spec[FORM]
        data.each { thing ->
            def modified = false
            if (form && isSubSet(c, form, thing)) {
                def allOpsSuccessful = spec[OPERATIONS].every { op ->
                    def path = op[PATH].findAll { it instanceof String }
                    def property = op[PROPERTY]
                    def repeatable = property in repeatableTerms
                    def delete = op[DELETE]
                    def insert = op[INSERT]
                    def localMappings = op[MAPPINGS]
                    def obj = DocumentUtil.getAtPath(thing, path, [], false)
                    if (localMappings) {
                        def anySuccessful = false
                        asList(obj).each { Map node ->
                            for (entry in localMappings) {
                                if (replace(node, property, entry.key, entry.value, c, repeatable)) {
                                    return anySuccessful = true
                                }
                            }
                        }
                        return anySuccessful
                    }
                    for (Map node in asList(obj)) {
                        if (delete && insert) {
                            if (replace(node, property, delete, insert, c, repeatable)) {
                                return true
                            }
                        } else if (delete) {
                            if (doDelete(node, property, delete, c)) {
                                return true
                            }
                        } else if (insert) {
                            if (doInsert(node, property, insert, c, repeatable)) {
                                return true
                            }
                        }
                    }
                }
                if (allOpsSuccessful) {
                    cleanUpEmpty(thing)
                    modified = true
                }
            }
            if (globalMappings) {
                modified = DocumentUtil.traverse(thing) { value, path ->
                    if (value instanceof String && globalMappings[value]) {
                        new DocumentUtil.Replace(globalMappings[value])
                    }
                }
            }
            if (modified) {
                updated.add(thing)
            }
        }

        return updated
    }

    static boolean isSubSet(DocumentComparator c, Map form, Map thing) {
        def copy = Document.deepCopy(form)
        def meta = copy[RECORD_KEY]
        if (meta) {
            meta.remove(ID_KEY)
            meta.remove(THING_KEY)
        }
        copy.remove(ID_KEY)
        DocumentUtil.traverse(copy) { value, path ->
            if (value instanceof String && value == MAPPINGS_PLACEHOLDER) {
                new DocumentUtil.Remove()
            }
        }
        return c.isSubset(copy, thing)
    }

    static boolean cleanUpEmpty(Map data) {
        DocumentUtil.traverse(data) { value, path ->
            if (value instanceof List || value instanceof Map) {
                if (value.isEmpty()) {
                    new DocumentUtil.Remove()
                }
            }
        }
    }

    static boolean replace(Map node, String property, Object delete, Object insert, DocumentComparator c, boolean repeatable) {
        return doDelete(node, property, delete, c) && doInsert(node, property, insert, c, repeatable)
    }

    static boolean doDelete(Map node, String property, Object delete, DocumentComparator c) {
        def current = node[property]
        if (delete instanceof String) {
            if (asList(current) == asList(delete)) {
                node.remove(property)
                return true
            }
        } else if (delete instanceof Map) {
            if (current instanceof List && current.removeAll { c.isEqual(it, delete) }) {
                return true
            } else if (current instanceof Map && c.isEqual(current, delete)) {
                node.remove(property)
                return true
            }
        } else if (delete instanceof List && current instanceof List) {
            def matching = current.findIndexValues { Map m -> delete.any { c.isEqual(m, it) } }
            if (matching.size() == delete.size()) {
                matching.reverse().each { i ->
                    current.remove(i.intValue())
                }
                return true
            }
        }
        return false
    }

    static boolean doInsert(Map node, String property, Object insert, DocumentComparator c, boolean repeatable) {
        def current = node[property]

        if (insert instanceof String && current && asList(current) != asList(insert)) {
            return false
        }
        if (insert instanceof Map && current instanceof Map && !c.isEqual(current, insert) && !repeatable) {
            return false
        }
        if (current instanceof List && !repeatable && !current.any { c.isEqual(it, insert) }) {
            return false
        }

        if (current instanceof List) {
            asList(insert).each { Map m ->
                if (!current.any { c.isEqual(m, it) }) {
                    current.add(m)
                }
            }
        } else {
            node[property] = repeatable ? asList(insert) : insert
        }

        return true
    }
}


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
        // TODO: Allow multiple mappings (both local and global)
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

    // TODO: Refactor
    static modify(List<Map> data, Map spec, DocumentComparator c, Set<String> repeatableTerms) {
        def updated = []
        def globalMappings = spec[MAPPINGS]
        def form = spec[FORM]
        def normalizedForm = form ? normalizedForm(form) : null
        data.each { thing ->
            def saveChanges = false
            if (form && c.isSubset(normalizedForm, thing)) {
                def allOpsSuccessful = spec[OPERATIONS].every { op ->
                    def property = op[PROPERTY]
                    def repeatable = property in repeatableTerms
                    def delete = op[DELETE]
                    def insert = op[INSERT]
                    def localMappings = op[MAPPINGS]
                    def parentForm = DocumentUtil.getAtPath(normalizedForm, op[PATH])
                    def genericPath = op[PATH].findAll { it instanceof String }
                    def obj = DocumentUtil.getAtPath(thing, genericPath, [], false)
                    def anySuccessful = false
                    if (localMappings) {
                        for (Map node in asList(obj)) {
                            for (entry in localMappings) {
                                parentForm[property] = entry.key
                                if (!c.isSubset(parentForm, node)) {
                                    parentForm.remove(property)
                                    continue
                                }
                                if (replace(node, property, entry.key, entry.value, c, repeatable)) {
                                    anySuccessful = true
                                    parentForm.remove(property)
                                    break
                                } else {
                                    return false
                                }
                            }
                        }
                        return anySuccessful
                    }
                    for (Map node in asList(obj)) {
                        if (!c.isSubset(parentForm, node)) {
                            continue
                        }
                        if (delete && insert) {
                            if (!isDeletable(node[property], delete, c)) {
                                continue
                            }
                            if (replace(node, property, delete, insert, c, repeatable)) {
                                adjustForm(parentForm, property, delete)
                                anySuccessful = true
                            } else {
                                return false
                            }
                        } else if (delete) {
                            if (!isDeletable(node[property], delete, c)) {
                                continue
                            }
                            if (doDelete(node, property, delete, c)) {
                                adjustForm(parentForm, property, delete)
                                anySuccessful = true
                            } else {
                                return false
                            }
                        } else if (insert) {
                            if (doInsert(node, property, insert, c, repeatable)) {
                                anySuccessful = true
                            } else {
                                return false
                            }
                        }
                    }
                    return anySuccessful
                }
                if (allOpsSuccessful) {
                    cleanUpEmpty(thing)
                    saveChanges = true
                }
            }
            if (globalMappings) {
                saveChanges = DocumentUtil.traverse(thing) { value, path ->
                    if (value instanceof String && globalMappings[value]) {
                        new DocumentUtil.Replace(globalMappings[value])
                    }
                }
            }
            if (saveChanges) {
                updated.add(thing)
            }
        }

        return updated
    }

    static void adjustForm(Map form, String property, Object delete) {
        if (form[property] instanceof List) {
            form[property] -= delete
            if (form[property].isEmpty()) {
                form.remove(property)
            }
        } else {
            form.remove(property)
        }
    }

    static boolean isDeletable(Object obj, Object delete, DocumentComparator c) {
        asList(delete).every { d -> asList(obj).any { c.isEqual(["x": it], ["x": d]) } }
    }

    static Map normalizedForm(Map form) {
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
        return copy
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


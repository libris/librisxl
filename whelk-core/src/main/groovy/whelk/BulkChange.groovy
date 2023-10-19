package whelk

import whelk.converter.JsonLdToTrigSerializer
import whelk.util.DocumentUtil
import whelk.util.DocumentComparator
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.THING_KEY

import static whelk.JsonLd.asList

class BulkChange {
    private static final String GRAPH = 'graph'
    private static final String DELETE = 'delete'
    private static final String INSERT = 'insert'

    private static final String MAPPINGS = 'mappings'
    private static final String MODIFY_AT_PATH = 'modifyAtPath'

    private static final String GRAPH_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"

    private static final String MAPPINGS_PLACEHOLDER = "MAPPINGS_PLACEHOLDER"

    private static final int LIMIT = 3

    Whelk whelk
    Map spec
    DocumentComparator comparator

    List<String> ids
    List<Map> data
    Map modifiedData

    BulkChange(Whelk whelk, Map spec, List<String> ids = [], boolean deleteRecords = false) {
        this.spec = spec
        this.whelk = whelk
        this.comparator = new DocumentComparator()
        this.ids = loadIds(ids, spec[GRAPH], whelk.jsonld.context)
        if (deleteRecords) {
            deleteSelection(whelk, this.ids)
        } else {
            this.data = loadData(whelk, this.ids)
            this.modifiedData = modify(data, spec, this.comparator)
            // TODO: Show diff, save new versions if diff looks ok
        }
    }

    static List<String> loadIds(List<String> inputIds, Map graph, Map context) {
        if (graph) {
            def queriedIds = sparqlQueryIds(graph, context)
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
            doc.data[ID_KEY] = id
            doc.data
        }
    }

    static List<String> sparqlQueryIds(Map graph, Map context, Map mappings = null) {
        def ttl = toTurtle(graph, context)
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

        QueryExecution qe = QueryExecutionFactory.sparqlService(Document.BASE_URI.toString() + "sparql", queryString)
        ResultSet res = qe.execSelect()
        return res.collect { it.get(GRAPH).toString() }
    }

    static List<String> separatePrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith("prefix") }
                .collect { it.join('\n') }
    }

    static String toTurtle(Map data, Map context = null) {
        def bytes = JsonLdToTrigSerializer.toTurtle(context, data).toByteArray()
        return new String(bytes, "UTF-8")
    }

    static String sparqlify(String ttl) {
        def substitutions = [
                "<$GRAPH_TMP_ID>": "?$GRAPH",
                "<$THING_TMP_ID>": "?mainEntity",
        ]
        return ttl.replace(substitutions)
    }

    static modify(List<Map> data, Map spec, DocumentComparator c) {
        def updated = []
        def mappings = spec[MAPPINGS]
        def template = spec[GRAPH]
        data.each { d ->
            if (mappings && !template) {
                def modified = DocumentUtil.traverse(d) { value, path ->
                    if (value instanceof String && mappings[value]) {
                        new DocumentUtil.Replace(mappings[value])
                    }
                }
                if (modified) {
                    updated.add(d)
                }
                return
            }
            if (template && isSubSet(c, template, d, mappings.asBoolean())) {
                def delete = spec[DELETE]
                def insert = spec[INSERT]
                def modified = false
                asList(DocumentUtil.getAtPath(d, spec[MODIFY_AT_PATH], [], false)).each { Map modifyAt ->
                    if (delete && insert) {
                        if (mappings) {
                            def prop = delete.keySet().find()
                            for (entry in mappings) {
                                def del = [(prop): entry.key]
                                def ins = [(prop): entry.value]
                                if (replace(modifyAt, del, ins, c)) {
                                    return modified = true
                                }
                            }
                        } else if (replace(modifyAt, delete, insert, c)) {
                            cleanUpEmpty(d)
                            modified = true
                        }
                    } else if (delete) {
                        if (doDelete(modifyAt, delete, c)) {
                            cleanUpEmpty(d)
                            modified = true
                        }
                    } else if (insert) {
                        if (doInsert(modifyAt, insert, c)) {
                            modified = true
                        }
                    }
                }
                if (modified) {
                    updated.add(d)
                }
            }
        }
        return updated
    }

    static boolean isSubSet(DocumentComparator c, Map template, Map data, boolean mappings) {
        def copy = Document.deepCopy(template)
        def (record, thing) = copy[GRAPH_KEY]
        record.remove(ID_KEY)
        record.remove(THING_KEY)
        if (thing) {
            thing.remove(ID_KEY)
        }
        if (mappings) {
            DocumentUtil.traverse(copy) { value, path ->
                if (value instanceof String && value == MAPPINGS_PLACEHOLDER) {
                    new DocumentUtil.Remove()
                }
            }
        }
        return c.isSubset(copy, data)
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

    static boolean replace(Map modifyAt, Map delete, Map insert, DocumentComparator c) {
        return doDelete(modifyAt, delete, c) && doInsert(modifyAt, insert, c)
    }

    static boolean doDelete(Map node, Map delete, DocumentComparator c) {
        def deleteKey = delete.keySet().find()
        def deleteAt = node[deleteKey]
        def deleteValue = delete[deleteKey]
        if (deleteValue instanceof String) {
            if (asList(deleteAt) == asList(deleteValue)) {
                node.remove(deleteKey)
                return true
            }
        } else if (deleteValue instanceof Map) {
            if (deleteAt instanceof List && deleteAt.removeAll { c.isEqual(it, deleteValue) }) {
                return true
            } else if (deleteAt instanceof Map && c.isEqual(deleteAt, deleteValue)) {
                node.remove(deleteKey)
                return true
            }
        }
        return false
    }

    static boolean doInsert(Map node, Map insert, DocumentComparator c) {
        def modified = false
        insert.each { k, v ->
            if (node[k] instanceof List) {
                if (!node[k].any { c.isEqual(it, v) }) {
                    node[k].add(v)
                    modified = true
                }
            } else if (!node[k]) {
                // TODO: asList if should be list
                node[k] = v
                modified = true
            } else if (!c.isEqual(node[k], v)) {
                // TODO: Check if multiple values allowed
                node[k] = [node[k], v]
                modified = true
            }
        }
        return modified
    }
}


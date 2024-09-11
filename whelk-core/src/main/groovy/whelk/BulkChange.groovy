package whelk

import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import whelk.converter.JsonLdToTrigSerializer
import whelk.history.DocumentVersion
import whelk.history.History
import whelk.util.DocumentComparator
import whelk.util.DocumentUtil

import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.asList

class BulkChange {
    private static final String MATCH_FORM = 'matchForm'
    private static final String TARGET_FORM = 'targetForm'
    private static final String ADD = 'add'
    private static final String REMOVE = 'remove'

    private static final String GRAPH_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"
    private static final String _ID = '_id'

    private static final String GRAPH = 'graph'

    private static final int SPARQL_RES_LIMIT = 0
    private static final int ITEMS_PREVIEW_LIMIT = 5

    private static final DocumentComparator comparator = new DocumentComparator()

    private String sparqlEndpoint

    private Map context
    private Set<String> repeatableTerms

    BulkChange(String sparqlEndpoint, Map context, Set<String> repeatableTerms) {
        this.sparqlEndpoint = sparqlEndpoint
        this.context = context
        this.repeatableTerms = repeatableTerms
    }

    Map buildPreview(Map bulkChangeObj) {
        Map formSpec = (Map) bulkChangeObj['bulkChangeSpecification']
        Map matchForm = (Map) formSpec[MATCH_FORM]
        Map targetForm = (Map) formSpec[TARGET_FORM]
        List<String> selection = sparqlQueryIds(matchForm).sort()
        List<String> exampleIds = selection.take(ITEMS_PREVIEW_LIMIT)
        List<Map> diff = getFormChangeSets(matchForm, targetForm)
        return [
                (JsonLd.TYPE_KEY): 'BulkChangePreview',
                'changeSets'     : [diff],
                // Pagination?
                'totalItems'     : selection.size(),
                'items'          : exampleIds
                // TODO
//                'items'        : getItemsPreview(exampleIds, whelk)
        ]
    }

    Map modify(Map thing, Map formSpec) {
        Map matchForm = (Map) formSpec[MATCH_FORM]
        Map targetForm = (Map) formSpec[TARGET_FORM]
        Map changesByPath = getChangesByPath(matchForm, targetForm)
        clearImplicitIds(matchForm)
        clearImplicitIds(targetForm)
        Map matchFormCopy = (Map) Document.deepCopy(matchForm)

        if (!comparator.isSubset(matchForm, thing)) {
            throw new Exception("${thing[ID_KEY]} does not match specified form")
        }

        for (e in changesByPath) {
            List path = e.key
            Map changes = e.value

            Modification m = new Modification(changes, matchFormCopy, targetForm)
            String property = path.last()
            List parentPath = path.dropRight(1)
            Map matchParentForm = (Map) DocumentUtil.getAtPath(matchFormCopy, parentPath)
            List noIdxParentPath = parentPath.findAll { it instanceof String }
            List<Map> objects = (List<Map>) DocumentUtil.getAtPath(thing, noIdxParentPath, [], false)
                    .with(JsonLd::asList)

            boolean anySuccessful = false
            for (Map node in objects) {
                // Make sure that we are operating on the right node
                if (!comparator.isSubset(matchParentForm, node)
                        || (!m.valuesToRemove && !containsValues(node[property], m.valuesToRemove))) {
                    continue
                }

                m.executeModification(node, property)

                if (m.failed) {
                    throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${path}")
                } else {
                    anySuccessful = true
                }
            }

            if (anySuccessful) {
                if (m.valuesToRemove) {
                    adjustForm(matchParentForm, property, m.valuesToRemove)
                }
            } else {
                throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${path}")
            }
        }

        // All operations were successful
        cleanUpEmpty(thing)

        return thing
    }

    static boolean containsValues(Object obj, List valuesToRemove) {
        return valuesToRemove.every { v -> asList(obj).any { isEqual(it, v) } }
    }

    static boolean isEqual(Object a, Object b) {
        return comparator.isEqual(["x": a], ["x": b])
    }

    static boolean cleanUpEmpty(Map data) {
        return DocumentUtil.traverse(data) { value, path ->
            if (value instanceof List || value instanceof Map) {
                if (value.isEmpty()) {
                    new DocumentUtil.Remove()
                }
            }
        }
    }

    static void adjustForm(Map form, String property, List valuesToRemove) {
        if (form[property] instanceof List) {
            form[property].removeAll { x -> valuesToRemove.any { y -> isEqual(x, y) } }
            if (form[property].isEmpty()) {
                form.remove(property)
            }
        } else {
            form.remove(property)
        }
    }

//    static List<Map> getItemsPreview(List<String> ids, Map formSpec, Whelk whelk) {
//        return ids.collect(loadData).collect {
//            ['changeSets': getChangeSets(it, modify((Map) Document.deepCopy(it), formSpec), whelk.jsonld)]
//        }
//    }
//
//    static List<Map> getChangeSets(Map before, Map after, JsonLd jsonLd) {
//        List<Map> changeSets = (List<Map>) [before, after]
//                .collect { new DocumentVersion(new Document(it), null, 'xl') } // arbitrary changedBy, changedIn?
//                .with { new History(it, jsonLd) }
//                .m_changeSetsMap
//                .get('changeSets')
//
//        changeSets[0]['version'] = before
//        changeSets[1]['version'] = after
//
//        return changeSets
//    }

    static List<Map> getFormChangeSets(Map matchForm, Map targetForm) {
        return [
                [
                        (JsonLd.TYPE_KEY): 'ChangeSet',
                        'version'        : matchForm,
                        'removedPaths'   : [],
                        'addedPaths'     : []
                ],
                [
                        (JsonLd.TYPE_KEY): 'ChangeSet',
                        'version'        : targetForm,
                        'removedPaths'   : collectRemovedPaths(matchForm, targetForm),
                        'addedPaths'     : collectAddedPaths(matchForm, targetForm)
                ]
        ]
    }

    static Map<List, Map> getChangesByPath(Map matchForm, Map targetForm) {
        Map<List, Map> changesByPath = [:]
        Closure dropLastIndex = { List path -> path.last() instanceof Integer ? path.dropRight(1) : path }
        collectRemovedPaths(matchForm, targetForm).groupBy(dropLastIndex).each { path, exactPath ->
            changesByPath[(List) path] = [(REMOVE): exactPath]
        }
        collectAddedPaths(matchForm, targetForm).groupBy(dropLastIndex).each { path, exactPath ->
            if (changesByPath.containsKey(path)) {
                changesByPath[path].put(ADD, exactPath)
            } else {
                changesByPath[path] = [(ADD): exactPath]
            }
        }
        return changesByPath
    }

    static List<List> collectAddedPaths(Map matchForm, targetForm) {
        return collectChangedPaths(targetForm, matchForm, [])
    }

    static List<List> collectRemovedPaths(Map matchForm, Map targetForm) {
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
            // Allow modifying list of strings? e.g. "label": ["x", "y"] -> ["x", "z"]
            if (a.any { !(it instanceof Map) } || b.any { !(it instanceof Map) }) {
                throw new Exception("Lists must only contain Map")
            }
            def changedPaths = []
            a.eachWithIndex { elem, i ->
                Map peer = (Map) b.find { it instanceof Map && it[_ID] == elem[_ID] }
                changedPaths.addAll(peer ? collectChangedPaths(elem, peer, path + i) : [path + i])
            }
            return changedPaths
        }

        if (a instanceof String && b instanceof String) {
            return [path]
        }

        // To be allowed?
        throw new Exception("Changing datatype of a value is not allowed.")
    }

    static void clearImplicitIds(Object o) {
        DocumentUtil.findKey(o, '_id') { v, p ->
            new DocumentUtil.Remove()
        }
    }

    List<String> sparqlQueryIds(Map form) {
        def ttl = toTurtle(form, context)
        def (prefixes, ttlGraph) = separatePrefixes(ttl)
        def graphPattern = sparqlify(ttlGraph)

        def queryString = """
            ${prefixes}

            SELECT DISTINCT ?${GRAPH} WHERE {
                ${GRAPH} ?${GRAPH} {
                    ${graphPattern}
                }
            }
        """

        if (SPARQL_RES_LIMIT > 0) {
            queryString += "\nLIMIT ${SPARQL_RES_LIMIT}"
        }

        QueryExecution qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, queryString)
        ResultSet res = qe.execSelect()
        return res.collect { it.get(GRAPH).toString() }
    }

    static String toTurtle(Map thing, Map context = null) {
        // Which form will the "match form" be? Record embedded in main entity or @graph form?
        List<Map> atGraphForm = [(Map) thing.remove(RECORD_KEY) ?: [:], thing]
        Map record = (Map) atGraphForm[0]
        def recordId = record.remove(ID_KEY)
        def thingId = thing.remove(ID_KEY)
        record[ID_KEY] = GRAPH_TMP_ID
        thing[ID_KEY] = THING_TMP_ID

        def ttl = JsonLdToTrigSerializer.toTurtle(context, atGraphForm)
                .toByteArray()
                .with { new String(it, UTF_8) }

        if (recordId) {
            record[ID_KEY] = recordId
        } else {
            record.remove(ID_KEY)
        }
        if (thingId) {
            thing[ID_KEY] = thingId
        } else {
            thing.remove(ID_KEY)
        }

        return ttl
    }

    static List<String> separatePrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith("prefix") }
                .collect { it.join('\n') }
    }

    static String sparqlify(String ttl) {
        def substitutions = [
                "<$GRAPH_TMP_ID>": "?$GRAPH",
                "<$THING_TMP_ID>": "?mainEntity",
        ]
        return ttl.replace(substitutions)
    }

    class Modification {
        boolean changed = false
        boolean failed = false

        List valuesToRemove
        List valuesToAdd

        Modification(Map changes, Map matchForm, Map targetForm) {
            this.valuesToRemove = asList(changes[REMOVE]).collect(p -> DocumentUtil.getAtPath(matchForm, (List) p))
            this.valuesToAdd = asList(changes[ADD]).collect { p -> DocumentUtil.getAtPath(targetForm, (List) p) }
        }

        void executeModification(Map node, String property) {
            if (valuesToRemove && valuesToAdd) {
                replace(node, property)
            } else if (valuesToRemove && !valuesToAdd) {
                remove(node, property)
            } else if (!valuesToRemove && valuesToAdd) {
                add(node, property)
            }
        }

        private void remove(Map node, String property) {
            try {
                doRemove(node, property)
            } catch (Exception ignored) {
                failed = true
            } finally {
                changed = true
            }
        }

        private void add(Map node, String property) {
            try {
                doAdd(node, property)
            } catch (Exception ignored) {
                failed = true
            }
        }

        private void replace(Map node, String property) {
            try {
                doReplace(node, property)
            } catch (Exception ignored) {
                failed = true
            } finally {
                changed = true
            }
        }

        private doRemove(Map node, String property) {
            def current = node[property]
            def removeVal = valuesToRemove.size() == 1 ? valuesToRemove[0] : valuesToRemove

            if (removeVal instanceof String) {
                node[property] = asList(current).findAll { it != removeVal }
                if (((List) node[property]).isEmpty()) {
                    node.remove(property)
                }
            } else if (removeVal instanceof Map) {
                if (current instanceof List) {
                    node[property] = current.findAll { !isEqual(it, removeVal) }
                    if (((List) node[property]).isEmpty()) {
                        node.remove(property)
                    }
                } else if (current instanceof Map) {
                    node.remove(property)
                }
            } else if (removeVal instanceof List) {
                // current must be List too since valuesToRemove holds at least two items and we assume that current
                // contains all of these
                node[property] = current.findAll { c -> !removeVal.any { isEqual(it, c) } }
                if (((List) node[property]).isEmpty()) {
                    node.remove(property)
                }
            }
        }

        private doAdd(Map node, String property) {
            def current = node[property]

            for (v in valuesToAdd) {
                if (!asList(current).contains(v)) {
                    if (current == null) {
                        current = property in repeatableTerms ? [v] : v
                    } else if (property in repeatableTerms) {
                        current = asList(current) + v
                    } else {
                        failed = true
                        return
                    }
                    changed = true
                }
            }

            node[property] = current
        }

        private doReplace(Map node, String property) {
            def current = node[property]
            def removeVal = valuesToRemove.size() == 1 ? valuesToRemove[0] : valuesToRemove

            if (removeVal instanceof String) {
                if (valuesToAdd.size() != 1) {
                    // Strings can only be added one at a time.
                    failed = true
                    return
                }
                String addVal = valuesToAdd[0]
                if (current instanceof List) {
                    node[property] = current.collect { it == removeVal ? addVal : it }
                } else if (current instanceof String) {
                    node[property] = addVal
                }
            } else if (removeVal instanceof Map) {
                if (current instanceof List) {
                    int updateAt = current.findIndexOf { isEqual(it, removeVal) }
                    current.remove(updateAt)
                    valuesToAdd.findAll { v -> !current.any { isEqual(it, v) } }
                            .eachWithIndex { v, i -> current.add(updateAt + i, v) }
                } else if (current instanceof Map) {
                    node[property] = valuesToAdd.size() == 1 ? valuesToAdd.first() : valuesToAdd
                }
            } else if (removeVal instanceof List) {
                current = (List<Map>) current
                List<Number> removeAt = current.findIndexValues { c -> removeVal.any { isEqual(it, c) } }
                int insertAt = removeAt.first().intValue()
                removeAt.reverse().each { n ->
                    current.remove(n.intValue())
                }
                valuesToAdd.eachWithIndex { v, i ->
                    current.add(insertAt + i, (Map) v)
                }
            }
        }
    }
}

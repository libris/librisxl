package whelk.datatool.form

import whelk.Document
import whelk.util.DocumentUtil

import static whelk.JsonLd.TYPE_KEY

class FormDiff {
    private static final String _ID = '_id'
    private static final String _TYPE = '_type'
    private static final String _EXACT_MATCH_TYPE = "_ExactMatch"

    final Map matchForm
    final Map targetForm

    final List<List> exactMatchPaths

    final List<List> addedPaths
    final List<List> removedPaths

    private Map<List, Map> removedAddedByPath

    FormDiff(Map matchForm, Map targetForm) {
        this.matchForm = matchForm
        this.targetForm = targetForm
        this.removedPaths = collectRemovedPaths()
        this.addedPaths = collectAddedPaths()
        this.exactMatchPaths = collectExactMatchPaths()
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

    Map getRemovedAddedByPath() {
        if (removedAddedByPath == null) {
            removedAddedByPath = [:]
            Closure dropLastIndex = { List path -> path.last() instanceof Integer ? path.dropRight(1) : path }
            removedPaths.groupBy(dropLastIndex).each { path, exactPaths ->
                removedAddedByPath[path] = ['remove': exactPaths]
            }
            addedPaths.groupBy(dropLastIndex).each { path, exactPaths ->
                if (removedAddedByPath.containsKey(path)) {
                    removedAddedByPath[path].put('add', exactPaths)
                } else {
                    removedAddedByPath[path] = [('add'): exactPaths]
                }
            }
        }
        return removedAddedByPath
    }

    private List<List> collectAddedPaths() {
        return collectChangedPaths(targetForm, matchForm, [])
    }

    private List<List> collectRemovedPaths() {
        return collectChangedPaths(matchForm, targetForm, [])
    }

    private List<List> collectExactMatchPaths() {
        List paths = []
        DocumentUtil.findKey(matchForm, _TYPE) { value, path ->
            if (value == _EXACT_MATCH_TYPE) {
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
            a.eachWithIndex { elem, i ->
                def peer = b.find { it == elem || it instanceof Map && it[_ID] == elem[_ID] }
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

    Map getMatchFormWithoutMarkers() {
        return withoutMarkers(matchForm)
    }

    Map getTargetFormWithoutMarkers() {
        return withoutMarkers(targetForm)
    }

    private static void clearMarkers(Object o) {
        DocumentUtil.traverse(o) { v, p ->
            if (v instanceof Map) {
                v.remove(_ID)
                v.remove(_TYPE)
                return new DocumentUtil.Nop()
            }
        }
    }

    static Map withoutMarkers(Map form) {
        var f = (Map) Document.deepCopy(form)
        clearMarkers(f)
        return f
    }
}

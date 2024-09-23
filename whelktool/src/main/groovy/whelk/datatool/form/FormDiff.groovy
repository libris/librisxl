package whelk.datatool.form

import whelk.Document
import whelk.datatool.util.DocumentComparator
import whelk.util.DocumentUtil

import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.getAtPath

class FormDiff {
    private static final DocumentComparator comparator = new DocumentComparator()

    private static final String _ID = '_id'
    private static final String _TYPE = '_type'
    private static final String _EXACT_MATCH_TYPE = "_ExactMatch"

    final Map matchForm
    final Map targetForm

    final Set<List> exactMatchPaths

    final List<List> addedPaths
    final List<List> removedPaths

    Map<List, List<Change>> changesByPath

    static enum MatchingMode {
        EXACT,
        SUBSET
    }

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

    Map<List, List<Change>> getChangesByPath() {
        if (changesByPath == null) {
            List<Change> changes = collectRemove() + collectAdd() as List<Change>
            changesByPath = changes.groupBy { it.path() }
        }
        return changesByPath
    }

    List<Remove> collectRemove() {
        return (List<Remove>) removedPaths.collect { fullPath ->
            asList(getAtPath(matchForm, fullPath)).collect { value ->
                new Remove(fullPath, value)
            }
        }.flatten()
    }

    List<Add> collectAdd() {
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

    private Set<List> collectExactMatchPaths() {
        Set paths = []
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

    static List dropLastIndex(List path) {
        return !path.isEmpty() && path.last() instanceof Integer ? path.dropRight(1) : path
    }

    static interface Change {
        List path()
        Object value()
    }

    class Remove implements Change {
        private final List path
        private final Object value
        private final MatchingMode matchingMode

        Remove(List fullPath, Object value) {
            if (value instanceof Map) {
                this.value = withoutMarkers(value)
                this.matchingMode = exactMatchPaths.contains(fullPath) ? MatchingMode.EXACT : MatchingMode.SUBSET
            } else {
                this.value = value
                this.matchingMode = MatchingMode.EXACT
            }
            this.path = dropLastIndex(fullPath)
        }

        boolean matches(Object o) {
            switch (matchingMode) {
                case MatchingMode.EXACT: return comparator.isEqual(value, o)
                case MatchingMode.SUBSET: return comparator.isSubset(value, o)
            }
        }

        @Override
        List path() {
            return path
        }

        @Override
        Object value() {
            return value
        }
    }

    static class Add implements Change {
        private final List path
        private final Object value

        // Should matching mode apply to Add too?
        Add(List fullPath, Object value) {
            this.value = value instanceof Map ? withoutMarkers(value) : value
            this.path = dropLastIndex(fullPath)
        }

        @Override
        List path() {
            return path
        }

        @Override
        Object value() {
            return value
        }
    }
}
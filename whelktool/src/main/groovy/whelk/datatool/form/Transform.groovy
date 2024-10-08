package whelk.datatool.form

import whelk.Document
import whelk.datatool.util.DocumentComparator
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.getAtPath

class Transform {
    private static final DocumentComparator comparator = new DocumentComparator()

    private static final String _ID = '_id'
    private static final String _MATCH = '_match'

    final Map matchForm
    final Map targetForm

    final Set<List> exactMatchPaths

    final List<List> addedPaths
    final List<List> removedPaths

    List<ChangesForNode> changes

    static enum MatchingMode {
        EXACT('Exact'),
        SUBSET('Subset')

        String str

        MatchingMode(String str) {
            this.str = str
        }
    }

    Transform(Map matchForm, Map targetForm) {
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

    List<ChangesForNode> getChanges() {
        if (changes == null) {
            def matchFormClean = getMatchFormWithoutMarkers()
            changes = (collectRemove() + collectAdd() as List<Change>)
                    .groupBy { it.parentPath() }
                    .collect { parentPath, changeList ->
                        new ChangesForNode(dropIndexes(parentPath),
                                getAtPath(matchFormClean, parentPath) as Map,
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
                        ? new Remove(fullPath, withoutMarkers(value), exactMatchPaths.contains(fullPath) ? MatchingMode.EXACT : MatchingMode.SUBSET)
                        : new Remove(fullPath, value, MatchingMode.EXACT)
            }
        }.flatten()
    }

    private List<Add> collectAdd() {
        return (List<Add>) addedPaths.collect { fullPath ->
            asList(getAtPath(targetForm, fullPath)).collect { value ->
                new Add(fullPath, value instanceof Map ? withoutMarkers(value) : value)
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
                def peer = b.find { bElem -> aElem == bElem || sameNode(aElem, bElem)}
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

    Map getMatchFormWithoutMarkers() {
        return withoutMarkers(matchForm)
    }

    private static void clearMarkers(Object o) {
        DocumentUtil.traverse(o) { v, p ->
            if (v instanceof Map) {
                v.remove(_ID)
                v.remove(_MATCH)
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

    static List<String> dropIndexes(List path) {
        return path.findAll { it instanceof String } as List<String>
    }

    // Need a better name for this...
    static class ChangesForNode {
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
            switch (matchingMode) {
                case MatchingMode.EXACT: return comparator.isEqual(form, node)
                case MatchingMode.SUBSET: return comparator.isSubset(form, node)
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

    static class Remove extends Change {
        private final MatchingMode matchingMode

        Remove(List path, Object value, MatchingMode matchingMode) {
            this.path = path
            this.value = value
            this.matchingMode = matchingMode
        }

        boolean matches(Object o) {
            switch (matchingMode) {
                case MatchingMode.EXACT: return comparator.isEqual(value, o)
                case MatchingMode.SUBSET: return comparator.isSubset(value, o)
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
}

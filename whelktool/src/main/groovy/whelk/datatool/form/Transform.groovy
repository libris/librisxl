package whelk.datatool.form

import groovy.transform.PackageScope
import whelk.Whelk
import whelk.datatool.util.DocumentComparator

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.datatool.form.MatchForm.ANY_TYPE
import static whelk.datatool.form.MatchForm.BNODE_ID
import static whelk.datatool.form.MatchForm.EXACT
import static whelk.datatool.form.MatchForm.HAS_ID
import static whelk.datatool.form.MatchForm.MATCHING_MODE
import static whelk.datatool.form.MatchForm.dropIndexes
import static whelk.util.DocumentUtil.getAtPath

class Transform {
    private static final DocumentComparator comparator = new DocumentComparator()

    MatchForm matchForm
    Map targetForm

    List<List> addedPaths
    List<List> removedPaths

    List<ChangesForNode> changes

    Transform(Map matchForm, Map targetForm, Whelk whelk) {
        this.matchForm = new MatchForm(matchForm, whelk)
        this.targetForm = targetForm
        this.removedPaths = collectRemovedPaths()
        this.addedPaths = collectAddedPaths()
    }

    Transform(Map matchForm, Map targetForm) {
        this(matchForm, targetForm, null)
    }

    List<Map> getChangeSets() {
        return [
                [
                        (TYPE_KEY)    : 'ChangeSet',
                        'version'     : matchForm.form,
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
            asList(getAtPath(matchForm.form, fullPath)).collect { value ->
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
        return collectChangedPaths(targetForm, matchForm.form, [])
    }

    private List<List> collectRemovedPaths() {
        return collectChangedPaths(matchForm.form, targetForm, [])
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
            this.propertyPath = dropIndexes(matchForm.formBNodeIdToPath[nodeId])
        }

        boolean matches(Map node) {
            return matchForm.matches(form(), node) && removeMatches(node)
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
            return getAtPath(matchForm.form, matchForm.formBNodeIdToPath[nodeId]) as Map
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
            return matchForm.matches(value, o) || (property() == TYPE_KEY && value == ANY_TYPE)
        }

        String parentId() {
            getAtPath(matchForm.form, parentPath())[BNODE_ID]
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
}

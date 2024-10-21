package whelk.datatool.form


import whelk.Document
import whelk.util.DocumentUtil

import static whelk.datatool.form.Transform.isSubset
import static whelk.datatool.form.Transform.isEqual
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.getAtPath

class ModifiedThing {
    private final Transform transform
    private final Set<String> repeatableTerms

    Map before
    Map after

    ModifiedThing(Map thing, Transform transform, Set<String> repeatableTerms) {
        this.transform = transform
        this.repeatableTerms = repeatableTerms
        this.before = (Map) Document.deepCopy(thing) // Maybe make it optional to keep an unmodified copy?
        this.after = modify(thing)
    }

    boolean isModified() {
        return !isEqual(before, after)
    }

    private Map modify(Map thing) {
        if (!transform.matches(thing)) {
            return thing
        }

        if (!transform.getMatchFormVariants().any { isMatch(it, thing) }) {
            return thing
        }

        // Map changes to nodes to which the changes are applicable (matching by form)
        Map<Transform.ChangesForNode, List<Map>> changeMap = transform.getChanges().collectEntries { cfn ->
            def candidateNodes = asList(getAtPath(thing, cfn.propertyPath, [], false)) as List<Map>
            return [cfn, candidateNodes.findAll { cNode -> cfn.matches(cNode) }]
        }

        // Perform changes node by node, property by property
        changeMap.each { c, matchingNodes ->
            matchingNodes.each { node ->
                c.changeList.groupBy { it.property() }.each { property, changeList ->
                    def removeList = changeList.findAll { it instanceof Transform.Remove } as List<Transform.Remove>
                    def addList = changeList.findAll { it instanceof Transform.Add } as List<Transform.Add>
                    try {
                        executeModification(node, property, removeList, addList)
                    } catch (Exception e) {
                        throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${c.propertyPath + property}: ${e.getMessage()}")
                    }
                }
            }
        }

        cleanUpEmpty(thing)

        return thing
    }

    private static boolean cleanUpEmpty(Map data) {
        return DocumentUtil.traverse(data) { value, path ->
            if (value instanceof List || value instanceof Map) {
                if (value.isEmpty()) {
                    new DocumentUtil.Remove()
                }
            }
        }
    }

    boolean isMatch(Map form, Map thing) {
        if (!isSubset(form, thing)) {
            return false
        }
        return transform.exactMatchPaths.every { ep ->
            getAtPath(thing, ep.findAll { it instanceof String }, [], false)
                    .with { asList(it) }
                    .any { isEqual(getAtPath(form, ep), it) }
        }
    }

    private void executeModification(Map node, String property, List<Transform.Remove> valuesToRemove,
                                     List<Transform.Add> valuesToAdd) {
        if (!valuesToRemove.isEmpty() && !valuesToAdd.isEmpty()) {
            replace(node, property, valuesToRemove, valuesToAdd)
        } else if (!valuesToRemove.isEmpty() && valuesToAdd.isEmpty()) {
            remove(node, property, valuesToRemove)
        } else if (valuesToRemove.isEmpty() && !valuesToAdd.isEmpty()) {
            add(node, property, valuesToAdd)
        }
    }

    private static void remove(Map node, String property, List<Transform.Remove> valuesToRemove) {
        def current = asList(node[property])
        // Assume that it has already been checked that current contains all valuesToRemove
        valuesToRemove.each { v -> current.removeAll { v.matches(property, it) } }
        if (current.isEmpty()) {
            node.remove(property)
        } else {
            node[property] = current
        }
    }

    private void add(Map node, String property, List<Transform.Add> valuesToAdd) {
        addRecursive(node, property, valuesToAdd.collect { it.value })
    }

    private void addRecursive(Map node, String property, List valuesToAdd) {
        def current = node[property]

        for (value in valuesToAdd) {
            if (!asList(current).any { isEqual(value, it) }) {
                if (current == null) {
                    current = property in repeatableTerms ? [value] : value
                } else if (property in repeatableTerms) {
                    current = asList(current) + value
                } else if (current instanceof Map && value instanceof Map) {
                    ((Map) value).each { k, v ->
                        addRecursive((Map) current, (String) k, asList(v))
                    }
                } else {
                    throw new Exception("Property $property is not repeatable.")
                }
            }
        }

        node[property] = current
    }

    private void replace(Map node, String property, List<Transform.Remove> valuesToRemove,
                         List<Transform.Add> valuesToAdd) {
        def current = asList(node[property])

        List<Number> removeAt = current.findIndexValues { c -> valuesToRemove.any { v -> v.matches(property, c) } }
        int insertAt = removeAt.first().intValue()

        removeAt.reverse().each { n ->
            current.remove(n.intValue())
        }
        valuesToAdd.collect { it.value }
                .findAll { v -> !current.any { isEqual(it, v) } }
                .eachWithIndex { v, i -> current.add(insertAt + i, v) }

        node[property] = current.size() == 1 && !repeatableTerms.contains(property)
                ? current.first()
                : current
    }

}

package whelk.datatool.form


import whelk.Document
import whelk.datatool.util.DocumentComparator
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.getAtPath

class ModifiedThing {
    private static final DocumentComparator comparator = new DocumentComparator()

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
        return !comparator.isEqual(before, after)
    }

    private Map modify(Map thing) {
        if (!transform.matchForm.matches(thing)) {
            return thing
        }

        def bySpecificitySort = { a, b ->
            b.shouldMatchExact() <=> a.shouldMatchExact()
                    ?: a.matchAnyType() <=> b.matchAnyType()
        }

        transform.getChanges().groupBy { it.propertyPath }.each { propPath, changes ->
            // Map changes to a node to which the changes are applicable (matching by form)
            Map<Transform.ChangesForNode, Map> changeMap = [:]

            // Prioritize matching ChangesForNode objects with a narrower hit range
            changes.sort(bySpecificitySort).each { cfn ->
                def candidateNodes = asList(getAtPath(thing, cfn.propertyPath, [], false)) as List<Map>
                def matchingNode = candidateNodes.find { cNode -> !cNode['_matched'] && cfn.matches(cNode) }
                if (matchingNode) {
                    // Mark node temporarily as matched to avoid mapping different ChangesForNode objects to the same node
                    matchingNode['_matched'] = true
                    changeMap[cfn] = matchingNode
                }
            }

            // Remove temporary markers
            changeMap.values().each { it.remove('_matched') }

            // Perform changes node by node, property by property
            changeMap.each { cfn, matchingNode ->
                cfn.changeList.groupBy { it.property() }.each { property, changeList ->
                    def removeList = changeList.findAll { it instanceof Transform.Remove }
                            .sort(bySpecificitySort) as List<Transform.Remove>
                    def addList = changeList.findAll { it instanceof Transform.Add } as List<Transform.Add>
                    try {
                        executeModification(matchingNode, property, removeList, addList)
                    } catch (Exception e) {
                        throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${cfn.propertyPath + property}: " +
                                "${e.getMessage()}")
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
        valuesToRemove.each { v ->
            if (v.hasId()) {
                current.removeAll { v.matches(property, it) }
            } else {
                current.find { v.matches(property, it) }
                        .with { current.remove(it) }
            }
        }
        if (current.isEmpty()) {
            node.remove(property)
        } else {
            node[property] = current
        }
    }

    private void add(Map node, String property, List<Transform.Add> valuesToAdd) {
        addRecursive(node, property, valuesToAdd)
    }

    private void addRecursive(Map node, String property, List<Transform.Add> valuesToAdd) {
        def current = node[property]

        for (add in valuesToAdd) {
            if (!asList(current).any { add.matches(it) }) {
                if (current == null) {
                    current = property in repeatableTerms ? [add.value] : add.value
                } else if (property in repeatableTerms) {
                    current = asList(current) + add.value
                } else if (current instanceof Map && add.value instanceof Map) {
                    ((Map) add.value).each { k, v ->
                        addRecursive((Map) current, (String) k, asList(v).collect { transform.newAddValue(it) })
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
        def removeAt = []
        valuesToRemove.each { v ->
            if (v.hasId()) {
                current.eachWithIndex { c, i ->
                    if (!removeAt.contains(i) && v.matches(property, c)) {
                        removeAt.add(i)
                    }
                }
            }
            for (i in 0..<current.size()) {
                if (!removeAt.contains(i) && v.matches(property, current[i])) {
                    removeAt.add(i)
                    break
                }
            }
        }
        removeAt.sort(true)
        int insertAt = removeAt.first() as int

        removeAt.reverseEach { i -> current.remove(i) }
        valuesToAdd.findAll { v -> !current.any { v.matches(it) } }
                .eachWithIndex { v, i -> current.add(insertAt + i, v.value) }

        node[property] = current.size() == 1 && !repeatableTerms.contains(property)
                ? current.first()
                : current
    }
}

package whelk.datatool.form


import whelk.Document
import whelk.datatool.util.DocumentComparator
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.getAtPath

class ModifiedThing {
    private static final DocumentComparator comparator = new DocumentComparator()

    private final FormDiff formDiff
    private final Set<String> repeatableTerms

    Map before
    Map after

    ModifiedThing(Map thing, FormDiff formDiff, Set<String> repeatableTerms) {
        this.formDiff = formDiff
        this.repeatableTerms = repeatableTerms
        this.before = (Map) Document.deepCopy(thing) // Maybe make it optional to keep an unmodified copy?
        this.after = modify(thing)
    }

    private Map modify(Map thing) {
        Map matchFormCopy = formDiff.getMatchFormWithoutMarkers()

        if (!isMatch(matchFormCopy, thing)) {
            return thing
        }

        formDiff.getChangesByPath()
        /*
        Sort by path size to get the most deeply nested changes first so that list indexes in paths will still be
        correct after we modify matchFormCopy (see adjustForm further down)
        */
                .sort { -it.key.size() }
                .each { path, changes ->
                    if (path.isEmpty()) {
                        thing = formDiff.getTargetFormWithoutMarkers()
                        return
                    }

                    String property = path.last()
                    List parentPath = path.dropRight(1)
                    Map matchParentForm = (Map) getAtPath(matchFormCopy, parentPath)
                    List noIdxParentPath = parentPath.findAll { it instanceof String }
                    List<Map> nodes = (List<Map>) getAtPath(thing, noIdxParentPath, [], false)
                            .with { asList(it) }
                    List<FormDiff.Remove> valuesToRemove = changes.findAll { it instanceof FormDiff.Remove } as List<FormDiff.Remove>
                    List<FormDiff.Add> valuesToAdd = changes.findAll { it instanceof FormDiff.Add } as List<FormDiff.Add>

                    for (Map node in nodes) {
                        // Make sure that we are operating on the right node
                        if ((parentPath in formDiff.exactMatchPaths && !isEqual(matchParentForm, node))
                                || !isSubset(matchParentForm, node)
                                || (!valuesToRemove.isEmpty() && !matchingValues(node[property], valuesToRemove))
                        ) {
                            continue
                        }

                        try {
                            executeModification(node, property, valuesToRemove, valuesToAdd)
                        } catch (Exception e) {
                            throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${path}: ${e.getMessage()}")
                        }
                    }

                    if (valuesToRemove) {
                        adjustForm(matchParentForm, property, valuesToRemove.collect { it.value() })
                    }
                }

        cleanUpEmpty(thing)

        return thing
    }

    private static boolean matchingValues(Object obj, List<FormDiff.Remove> valuesToRemove) {
        return valuesToRemove.every { v -> asList(obj).any { v.matches(it) } }
    }

    private static boolean isSubset(Object a, Object b) {
        return comparator.isSubset(a, b)
    }

    private static boolean isEqual(Object a, Object b) {
        return comparator.isEqual(a, b)
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

    private static void adjustForm(Map form, String property, List valuesToRemove) {
        if (form[property] instanceof List) {
            form[property].removeAll { x -> valuesToRemove.any { y -> isEqual(x, y) } }
            if (form[property].isEmpty()) {
                form.remove(property)
            }
        } else {
            form.remove(property)
        }
    }

    boolean isMatch(Map form, Map thing) {
        if (!isSubset(form, thing)) {
            return false
        }
        return formDiff.exactMatchPaths.every { ep ->
            getAtPath(thing, ep.findAll { it instanceof String }, [], false)
                    .with { asList(it) }
                    .any { isEqual(getAtPath(form, ep), it) }
        }
    }

    private void executeModification(Map node, String property, List<FormDiff.Remove> valuesToRemove,
                                     List<FormDiff.Add> valuesToAdd) {
        if (!valuesToRemove.isEmpty() && !valuesToAdd.isEmpty()) {
            replace(node, property, valuesToRemove, valuesToAdd)
        } else if (!valuesToRemove.isEmpty() && valuesToAdd.isEmpty()) {
            remove(node, property, valuesToRemove)
        } else if (valuesToRemove.isEmpty() && !valuesToAdd.isEmpty()) {
            add(node, property, valuesToAdd)
        }
    }

    private static void remove(Map node, String property, List<FormDiff.Remove> valuesToRemove) {
        def current = asList(node[property])
        // Assume that it has already been checked that current contains all valuesToRemove
        valuesToRemove.each { v -> current.removeAll { v.matches(it) } }
        if (current.isEmpty()) {
            node.remove(property)
        } else {
            node[property] = current
        }
    }

    private void add(Map node, String property, List<FormDiff.Add> valuesToAdd) {
        addRecursive(node, property, valuesToAdd.collect { it.value() })
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

    private void replace(Map node, String property, List<FormDiff.Remove> valuesToRemove,
                         List<FormDiff.Add> valuesToAdd) {
        def current = asList(node[property])

        List<Number> removeAt = current.findIndexValues { c -> valuesToRemove.any { v -> v.matches(c) } }
        int insertAt = removeAt.first().intValue()

        removeAt.reverse().each { n ->
            current.remove(n.intValue())
        }
        valuesToAdd.collect { it.value() }
                .findAll { v -> !current.any { isEqual(it, v) } }
                .eachWithIndex { v, i -> current.add(insertAt + i, v) }

        node[property] = current.size() == 1 && !repeatableTerms.contains(property)
                ? current.first()
                : current
    }
}

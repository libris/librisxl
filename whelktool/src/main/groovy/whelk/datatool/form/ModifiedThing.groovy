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

        formDiff.getRemovedAddedByPath().each { path, removedAdded ->
            Modification m = new Modification(removedAdded, matchFormCopy, formDiff.getTargetFormWithoutMarkers())
            String property = path.last()
            List parentPath = path.dropRight(1)
            Map matchParentForm = (Map) getAtPath(matchFormCopy, parentPath)
            List noIdxParentPath = parentPath.findAll { it instanceof String }
            List<Map> nodes = (List<Map>) getAtPath(thing, noIdxParentPath, [], false)
                    .with { asList(it) }

            for (Map node in nodes) {
                // Make sure that we are operating on the right node
                if (!comparator.isSubset(matchParentForm, node)
                        || (!m.removeAny && m.valuesToRemove && !containsValues(node[property], m.valuesToRemove))) {
                    continue
                }

                try {
                    m.executeModification(node, property)
                } catch (Exception e) {
                    throw new Exception("Failed to modify ${thing[ID_KEY]} at path ${path}: ${e.getMessage()}")
                }
            }

            if (m.valuesToRemove) {
                adjustForm(matchParentForm, property, m.valuesToRemove)
            }
        }

        cleanUpEmpty(thing)

        return thing
    }

    private static boolean containsValues(Object obj, List valuesToRemove) {
        return valuesToRemove.every { v -> asList(obj).any { isEqual(it, v) } }
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

    boolean isMatch(Map matchForm, Map thing) {
        if (!comparator.isSubset(matchForm, thing)) {
            return false
        }
        return formDiff.exactMatchPaths.every {ep ->
            getAtPath(thing, ep.collect { it instanceof String }, [], false)
                    .with { asList(it) }
                    .any { isEqual(getAtPath(matchForm, ep), it) }
        }
    }


    private class Modification {
        List valuesToRemove
        List valuesToAdd
        boolean removeAny

        Modification(Map removedAdded, Map matchForm, Map targetForm) {
            this.valuesToRemove = removedAdded['remove']
                    ?.collect { p -> getAtPath(matchForm, (List) p) }
                    ?.flatten()
            this.valuesToAdd = removedAdded['add']
                    ?.collect { p -> getAtPath(targetForm, (List) p) }
                    ?.flatten()
            this.removeAny = valuesToRemove?.size() == 1 && !valuesToRemove.first()
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
            if (removeAny) {
                node.remove(property)
            } else {
                def current = asList(node[property])
                // Assume that it has already been checked that current contains all valuesToRemove
                valuesToRemove.each { v -> current = current.findAll { !isEqual(it, v) } }
                if (current.isEmpty()) {
                    node.remove(property)
                } else {
                    node[property] = current
                }
            }
        }

        private void add(Map node, String property) {
            addRecursive(node, property, valuesToAdd)
        }

        private void addRecursive(Map node, String property, List valuesToAdd) {
            def current = node[property]

            for (value in valuesToAdd) {
                if (!asList(current).any { isEqual(it, value) }) {
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

        private void replace(Map node, String property) {
            if (removeAny) {
                node[property] = valuesToAdd.size() == 1 && !repeatableTerms.contains(property)
                        ? valuesToAdd.first()
                        : valuesToAdd
            } else {
                def current = asList(node[property])

                List<Number> removeAt = current.findIndexValues { c -> valuesToRemove.any { isEqual(it, c) } }
                int insertAt = removeAt.first().intValue()

                removeAt.reverse().each { n ->
                    current.remove(n.intValue())
                }
                valuesToAdd.findAll { v -> !current.any { isEqual(it, v) } }
                        .eachWithIndex { v, i -> current.add(insertAt + i, v) }

                node[property] = current.size() == 1 && !repeatableTerms.contains(property)
                        ? current.first()
                        : current
            }
        }
    }
}

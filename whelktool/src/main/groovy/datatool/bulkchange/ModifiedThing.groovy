package datatool.bulkchange

import datatool.util.DocumentComparator
import whelk.Document
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.asList

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
        Map matchFormCopy = matchFormCopy()

        if (!comparator.isSubset(matchFormCopy, thing)) {
            throw new Exception("${thing[ID_KEY]} does not match specified form")
        }

        formDiff.getRemovedAddedByPath().each { path, removedAdded ->
            Modification m = new Modification(removedAdded, matchFormCopy, formDiff.targetForm)
            String property = path.last()
            List parentPath = path.dropRight(1)
            Map matchParentForm = (Map) DocumentUtil.getAtPath(matchFormCopy, parentPath)
            List noIdxParentPath = parentPath.findAll { it instanceof String }
            List<Map> nodes = (List<Map>) DocumentUtil.getAtPath(thing, noIdxParentPath, [], false)
                    .with { asList(it) }

            boolean anySuccessful = false
            for (Map node in nodes) {
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

    private Map matchFormCopy() {
        return (Map) Document.deepCopy(formDiff.matchForm)
    }

    private static boolean containsValues(Object obj, List valuesToRemove) {
        return valuesToRemove.every { v -> asList(obj).any { isEqual(it, v) } }
    }

    private static boolean isEqual(Object a, Object b) {
        return comparator.isEqual(["x": a], ["x": b])
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


    private class Modification {
        boolean changed = false
        boolean failed = false

        List valuesToRemove
        List valuesToAdd

        Modification(Map removedAdded, Map matchForm, Map targetForm) {
            this.valuesToRemove = asList(removedAdded['remove']).collect { p -> DocumentUtil.getAtPath(matchForm, (List) p) }
            this.valuesToAdd = asList(removedAdded['add']).collect { p -> DocumentUtil.getAtPath(targetForm, (List) p) }
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

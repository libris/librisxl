package whelk.util

import whelk.JsonLd

class DocumentUtil {
    public final static Operation NOP = new Nop()

    interface Visitor {
        Operation visitElement(def value, List path)
    }

    interface Linker {
        /**
         * Called for every blank node found in search
         *
         * @param blankNode
         * @param existingLinks List of sibling node @ids. Can be used for disambiguation.
         * @return
         */
        List<Map> link(Map blankNode, List existingLinks)

        /**
         * This is called when the blank node search encounters
         * a single string value where there would normally be a node
         */
        List<Map> link(String blank, List existingLinks)
    }

    /**
     * Traverse a JSON-LD structure in depth-first order
     *
     * @param data JSON-LD structure
     * @param visitor function to call for every value
     * @return true if data was changed
     */
    static boolean traverse(data, Visitor visitor) {
        return new DFS().traverse(data, visitor)
    }

    /**
     * Search for a key in JSON-LD structure
     *
     * @param data JSON-LD structure
     * @param key
     * @param visitor function to call with value for found keys
     * @return true if obj was changed
     */
    static boolean findKey(data, String key, Visitor visitor) {
        return findKey(data, [key], visitor)
    }

    /**
     * Search keys in JSON-LD structure
     *
     * @param data JSON-LD structure
     * @param keys
     * @param visitor function to call with value for found keys
     * @return true if obj was changed
     */
    static boolean findKey(data, Collection<String> keys, Visitor visitor) {
        Set<String> k = keys instanceof Set ? keys : new HashSet<>(keys)
        return traverse(data, { value, path ->
            if (path && path.last() instanceof String && k.contains(path.last())) {
                return visitor.visitElement(value, path)
            }
        })
    }

    static Visitor link(Linker linker, List<Map> disambiguationNodes = []) {
        return { value, path ->
            return DocumentUtil.&linkBlankNodes(value, linker, disambiguationNodes)
        }
    }

    /**
     *
     * @param objectOrArray
     * @param linker
     * @return
     */
    static Operation linkBlankNodes(def objectOrArray, Linker linker, List<Map> disambiguationNodes = []) {
        return linkBlankNode(objectOrArray, linker, disambiguationNodes)
    }

    static boolean isBlank(Map node) {
        return !node.containsKey('@id')
    }

    /**
     *
     * @param item
     * @param path
     * @param defaultTo
     * @return
     */
    static def getAtPath(item, Iterable path, defaultTo = null) {
        if (!item) {
            return defaultTo
        }

        for (int i = 0; i < path.size(); i++) {
            def p = path[i]
            if (p == '*') {
                if (item instanceof Collection) {
                    return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
                } else {
                    return []
                }
            } else if (((item instanceof Collection && p instanceof Integer) || item instanceof Map) && item[p] != null) {
                item = item[p]
            } else {
                return defaultTo
            }
        }
        return item
    }

    private static Operation linkBlankNode(List<Map> nodes, Linker linker, List<Map> disambiguationNodes = []) {
        if (!nodes.any(DocumentUtil.&isBlank)) {
            return NOP
        }

        List<Map> existingLinks = collectIris(nodes)
        List<Map> result = []

        List<Map> newLinked
        for (node in nodes) {
            if (isDefective(node)) {
                continue // remove node
            }
            if (isBlank(node) && (newLinked = linker.link(node, existingLinks) ?: linker.link(node, collectIris(disambiguationNodes)))) {
                result.addAll(newLinked.findAll { l ->
                    !existingLinks.contains(l['@id']) && !result.contains { it['@id'] == l['@id'] }
                })
            } else {
                result.add(node)
            }
        }

        if (nodes != result) {
            return new Replace(result)
        } else {
            return NOP
        }
    }

    private static Operation linkBlankNode(Map node, Linker linker, List<Map> disambiguationNodes = []) {
        if (isDefective(node)) {
            return new Remove()
        }
        if (!isBlank(node)) {
            return NOP
        }

        toOperation(linker.link(node, collectIris(disambiguationNodes)))
    }

    private static Operation linkBlankNode(String singleValue, Linker linker, List<Map> disambiguationNodes = []) {
        toOperation(linker.link(singleValue, collectIris(disambiguationNodes)))
    }

    private static Operation toOperation(List<Map> replacement) {
        replacement
                ? replacement.size() > 1 ? new Replace(replacement) : new Replace(replacement[0])
                : NOP
    }

    private static boolean isDefective(Map node) {
        node.size() == 0 || (node.size() == 1 && node.containsKey(JsonLd.TYPE_KEY))
    }

    private static List<Map> collectIris(List<Map> nodes) {
        nodes.findAll { !isBlank(it) }.collect { it['@id'] }
    }

    private static class DFS {
        Stack path
        Visitor visitor
        List operations

        boolean traverse(obj, Visitor visitor) {
            this.visitor = visitor
            path = new Stack()
            operations = []

            node(obj)
            operations = operations.reverse().each { it.perform(obj) }
            return !operations.isEmpty()
        }

        private void node(obj) {
            Operation op = visitor.visitElement(obj, Collections.unmodifiableList(path))
            if (op && !(op instanceof Nop)) {
                op.setPath(path)
                operations.add(op)
            }

            if (obj instanceof Map) {
                descend(((Map) obj).entrySet().collect({ new Tuple2(it.value, it.key) }))
            } else if (obj instanceof List) {
                descend(((List) obj).withIndex())
            }
        }

        private void descend(List<Tuple2> nodes) {
            for (n in nodes) {
                path.push(n.v2)
                node(n.v1)
                path.pop()
            }
        }
    }


    static abstract class Operation {
        List path

        protected abstract void perform(obj)

        protected Operation setPath(List path) {
            this.path = path.collect()
            return this
        }

        protected def parentAndKey(obj) {
            def p = path.collect()
            while (p.size() > 1) {
                obj = obj[p.remove(0)]
                if (obj == null) {
                    // already gone
                    return [null, null]
                }
            }
            return [obj, p[0]]
        }
    }

    static class Nop extends Operation {
        @Override
        protected void perform(Object obj) {}

        protected Operation setPath(List path) { this }
    }

    static class Remove extends Operation {
        @Override
        protected void perform(Object obj) {
            def (parent, key) = parentAndKey(obj)
            if (!parent) {
                return
            }

            parent.remove(key)
            if (parent.isEmpty() && path.size() > 1) {
                new Remove().setPath(path.collect()[0..-2]).perform(obj)
            }
        }
    }

    static class Replace extends Operation {
        def with

        Replace(with) {
            this.with = with
        }

        @Override
        protected void perform(Object obj) {
            def (parent, key) = parentAndKey(obj)
            if (parent != null) {
                parent[key] = with
            }
        }
    }
}
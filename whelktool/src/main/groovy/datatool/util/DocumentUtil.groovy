package datatool.util

class DocumentUtil {
    public final static Operation NOP = new Nop()

    interface Visitor {
        Operation visitElement(def value, List path)
    }

    interface Linker {
        List<Map> link(Map blankNode, List existingLinks)
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
        return traverse(data, { value, path ->
            if (path && path.last() == key) {
                return visitor.visitElement(value, path)
            }
        })
    }

    static Visitor link(Linker linker) {
        return { value, path ->
            return DocumentUtil.&linkBlankNodes(value, linker)
        }
    }

    /**
     *
     * @param objectOrArray
     * @param linker
     * @return
     */
    static Operation linkBlankNodes(def objectOrArray, Linker linker) {
        return linkBlankNode(objectOrArray, linker)
    }

    static boolean isBlank(Map node) {
        return !node.containsKey('@id')
    }

    private static Operation linkBlankNode(List<Map> nodes, Linker linker) {
        if (!nodes.any(DocumentUtil.&isBlank)) {
            return NOP
        }

        List<Map> existingLinks = nodes.findAll { !DocumentUtil.&isBlank(it) }.collect { it['@id'] }
        List<Map> result = []

        List<Map> newLinked
        for (node in nodes) {
            if (isBlank(node) && (newLinked = linker.link(node, existingLinks))) {
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

    private static Operation linkBlankNode(Map node, Linker mapper) {
        List<Map> replacement
        if (isBlank(node) && (replacement = mapper.link(node, []))) {
            return replacement.size() > 1 ? new Replace(replacement) : new Replace(replacement[0])
        }
        return NOP
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
            operations = operations.reverse().each { it.perform(obj )}
            return !operations.isEmpty()
        }

        private void node(obj) {
            Operation op = visitor.visitElement(obj, path.collect())
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
                path.push(n.second)
                node(n.first)
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
            if(!parent) {
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
package datatool.util

class DocumentUtil {
    public final static Operation NOP = new Nop()

    interface Visitor {
        Operation visitElement(List path, value)
    }

    interface Linker {
        List<Map> link(Map blankNode, List existingLinks)
    }

    /**
     * Traverse obj in depth-first order
     *
     * @param obj list or map containing nested lists or maps
     * @param visitor function to call for every node
     * @return true if obj was changed
     */
    static boolean traverse(obj, Visitor visitor) {
        return new DFS().traverse(obj, visitor)
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
            Operation op = visitor.visitElement(path, obj)
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

        protected void setPath(List path) {
            this.path = path.collect()
        }

        protected def parentAndKey(obj) {
            def p = path.collect()
            while (p.size() > 1) {
                obj = obj[p.remove(0)]
            }
            return [obj, p[0]]
        }
    }

    static class Nop extends Operation {
        @Override
        protected void perform(Object obj) {}
        protected void setPath(List path) {}
    }

    static class Remove extends Operation {
        @Override
        protected void perform(Object obj) {
            def (parent, key) = parentAndKey(obj)
            parent.remove(key)
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
            parent[key] = with
        }
    }
}
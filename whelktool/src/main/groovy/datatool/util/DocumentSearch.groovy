package datatool.util

class DocumentSearch {
    public final static Operation NOP = new Nop()

    interface NodeVisitor {
        Operation node(List path, value)
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

    Stack path
    NodeVisitor visitor
    List operations

    /**
     * Perform a depth-first search of obj
     *
     * @param obj list or map containing nested lists or maps
     * @param visitor function to call for every node
     * @return true if obj was changed
     */
    boolean search(obj, NodeVisitor visitor) {
        this.visitor = visitor
        path = new Stack()
        operations = []

        node(obj)
        operations = operations.reverse().each { it.perform(obj )}
        return !operations.isEmpty()
    }

    private void node(obj) {
        Operation op = visitor.node(path, obj)
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
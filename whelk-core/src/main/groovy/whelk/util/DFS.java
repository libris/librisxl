package whelk.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.Stack;

// This is a straightforward port of the old groovy version in DocumentUtil. Can probably be improved
class DFS {
    private DocumentUtil.Visitor visitor;
    private List<DocumentUtil.Operation> operations;

    private record Node(Object value, List<Object> path) {}

    public boolean traverse(final Object obj, DocumentUtil.Visitor visitor) {
        this.visitor = visitor;
        operations = new ArrayList<>();

        traverse(obj);

        Collections.reverse(operations);
        for (DocumentUtil.Operation op : operations) {
            op.perform(obj);
        }
        return !operations.isEmpty();
    }

    private void traverse(Object obj) {
        var stack = new Stack<Node>();

        stack.push(new Node(obj, new ArrayList<>()));

        do {
            var node = stack.pop();

            var op = visitor.visitElement(node.value, Collections.unmodifiableList(node.path));
            if (op != null && !(op instanceof DocumentUtil.Nop)) {
                op.setPath(node.path);
                operations.add(op);
            }

            if (node.value instanceof SequencedMap<?, ?> map) {
                for (var e : map.reversed().entrySet()) {
                    var path = new ArrayList<>(node.path.size() + 1);
                    path.addAll(node.path);
                    path.add(e.getKey());
                    stack.push(new Node(e.getValue(), path));
                }
            } else if (node.value instanceof List<?> list) {
                for (int i = list.size() - 1 ; i >= 0 ; i--) {
                    var path = new ArrayList<>(node.path.size() + 1);
                    path.addAll(node.path);
                    path.add(i);
                    stack.push(new Node(list.get(i), path));
                }
            } else if (node.value instanceof Map<?, ?> map) {
                for (var e : map.entrySet()) {
                    var path = new ArrayList<>(node.path.size() + 1);
                    path.addAll(node.path);
                    path.add(e.getKey());
                    stack.push(new Node(e.getValue(), path));
                }
            }
        } while (!stack.isEmpty());
    }
}

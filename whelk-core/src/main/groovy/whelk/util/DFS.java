package whelk.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

// This is a straightforward port of the old groovy version in DocumentUtil. Can probably be improved
class DFS {
    private Stack<Object> path;
    private DocumentUtil.Visitor visitor;
    private List<DocumentUtil.Operation> operations;

    private record Node(Object value, Object keyOrIndex) {}

    public boolean traverse(final Object obj, DocumentUtil.Visitor visitor) {
        this.visitor = visitor;
        path = new Stack<>();
        operations = new ArrayList<>();

        node(obj);

        Collections.reverse(operations);
        for  (DocumentUtil.Operation op : operations) {
            op.perform(obj);
        };
        return !operations.isEmpty();
    }

    private void node(Object obj) {
        var op = visitor.visitElement(obj, Collections.unmodifiableList(path));
        if (op != null && !(op instanceof DocumentUtil.Nop)) {
            op.setPath(path);
            operations.add(op);
        }

        if (obj instanceof Map<?, ?> map) {
            var nodes = map.entrySet().stream()
                    .map(e -> new Node(e.getValue(), e.getKey()))
                    .toList();
            descend(nodes);
        } else if (obj instanceof List<?> list) {
            var nodes = new ArrayList<Node>(list.size());
            for (int i = 0 ; i < list.size() ; i++) {
                nodes.add(new Node(list.get(i), i));
            }
            descend(nodes);
        }
    }

    private void descend(List<Node> nodes) {
        for (var n : nodes) {
            path.push(n.keyOrIndex);
            node(n.value);
            path.pop();
        }
    }
}

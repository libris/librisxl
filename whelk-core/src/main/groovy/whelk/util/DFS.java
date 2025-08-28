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
        var queue = new ArrayList<Node>();

        queue.add(new Node(obj, new ArrayList<>()));

        do {
            var node = queue.removeFirst();

            var op = visitor.visitElement(node.value, Collections.unmodifiableList(node.path));
            if (op != null && !(op instanceof DocumentUtil.Nop)) {
                op.setPath(node.path);
                operations.add(op);
            }

            if (node.value instanceof Map<?, ?> map) {
                var newQueue = new ArrayList<Node>(queue.size() + map.size());
                for (var e : map.entrySet()) {
                    var path = new ArrayList<>(node.path.size() + 1);
                    path.addAll(node.path);
                    path.add(e.getKey());
                    newQueue.add(new Node(e.getValue(), path));
                }
                newQueue.addAll(queue);
                queue = newQueue;
            }
            else if (node.value instanceof List<?> list) {
                var newQueue = new ArrayList<Node>(queue.size() + list.size());
                for (int i = 0 ; i < list.size(); i++) {
                    var path = new ArrayList<>(node.path.size() + 1);
                    path.addAll(node.path);
                    path.add(i);
                    newQueue.add(new Node(list.get(i), path));
                }
                newQueue.addAll(queue);
                queue = newQueue;
            }
        } while (!queue.isEmpty());
    }
}

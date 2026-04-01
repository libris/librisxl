package whelk.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// This is a straightforward port of the old groovy version in DocumentUtil. Can probably be improved
class DFS {
    private ArrayList<Object> path;
    private DocumentUtil.Visitor visitor;
    private List<DocumentUtil.Operation> operations;

    public boolean traverse(final Object obj, DocumentUtil.Visitor visitor) {
        this.visitor = visitor;
        path = new ArrayList<>();
        operations = new ArrayList<>();

        node(obj);

        Collections.reverse(operations);
        for (DocumentUtil.Operation op : operations) {
            op.perform(obj);
        }
        return !operations.isEmpty();
    }

    private void node(Object obj) {
        var op = visitor.visitElement(obj, path);
        if (op != null && !(op instanceof DocumentUtil.Nop)) {
            op.setPath(path);
            operations.add(op);
        }

        if (obj instanceof Map<?, ?> map) {
            for (var e : map.entrySet()) {
                path.add(e.getKey());
                node(e.getValue());
                path.removeLast();
            }
        } else if (obj instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                path.add(i);
                node(list.get(i));
                path.removeLast();
            }
        }
    }
}

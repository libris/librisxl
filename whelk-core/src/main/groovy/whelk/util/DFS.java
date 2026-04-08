package whelk.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Iterative depth-first traversal of JSON-LD structures.
class DFS {
    private static final int INITIAL_CAPACITY = 16;

    private DocumentUtil.Visitor visitor;
    private List<DocumentUtil.Operation> operations;

    private Object[] path = new Object[INITIAL_CAPACITY];
    private int pathSize = 0;

    private Object[] containerStack = new Object[INITIAL_CAPACITY]; // Map or List
    private Object[][] keysStack = new Object[INITIAL_CAPACITY][];  // Map keys (null for Lists)
    private int[] indexStack = new int[INITIAL_CAPACITY];
    private int stackSize = 0;

    private final List<Object> pathView = new AbstractList<>() {
        @Override
        public Object get(int index) {
            return path[index];
        }

        @Override
        public int size() {
            return pathSize;
        }
    };

    public boolean traverse(final Object obj, DocumentUtil.Visitor visitor) {
        this.visitor = visitor;
        operations = new ArrayList<>();

        pathSize = 0;
        stackSize = 0;

        visit(obj);
        push(obj);

        while (stackSize > 0) {
            int pos = stackSize - 1;

            int containerSize = keysStack[pos] != null
                    ? ((Map<?,?>) containerStack[pos]).size()
                    : ((List<?>) containerStack[pos]).size();

            if (indexStack[pos] >= containerSize) {
                // pop
                stackSize--;
                if (pathSize > 0) { // root has no path
                    pathSize--;
                    path[pathSize] = null;
                }
                containerStack[stackSize] = null;
                keysStack[stackSize] = null;
                continue;
            }

            Object key;
            Object value;

            int ix = indexStack[pos];
            if (keysStack[pos] != null) { // Map
                key = keysStack[pos][ix];
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) containerStack[pos];
                value = map.get(key);
            } else { // List
                key = ix;
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) containerStack[pos];
                value = list.get(ix);
            }
            indexStack[pos]++;

            ensurePathCapacity(pathSize + 1);
            path[pathSize++] = key;

            visit(value);

            if (value instanceof Map || value instanceof List) {
                push(value);
            } else { // leaf
                pathSize--;
                path[pathSize] = null;
            }
        }

        Collections.reverse(operations);
        for (DocumentUtil.Operation op : operations) {
            op.perform(obj);
        }
        return !operations.isEmpty();
    }

    private void visit(Object obj) {
        var op = visitor.visitElement(obj, pathView);
        if (op != null && !(op instanceof DocumentUtil.Nop)) {
            op.setPath(pathView);
            operations.add(op);
        }
    }

    private void push(Object container) {
        ensureCapacity(stackSize + 1);
        containerStack[stackSize] = container;
        indexStack[stackSize] = 0;

        if (container instanceof Map<?, ?> map) {
            keysStack[stackSize] = map.keySet().toArray();
            stackSize++;
        } else if (container instanceof List<?>) {
            keysStack[stackSize] = null;
            stackSize++;
        }
    }

    private void ensureCapacity(int min) {
        if (min > containerStack.length) {
            int newCap = Math.max(min, containerStack.length * 2);
            containerStack = Arrays.copyOf(containerStack, newCap);
            keysStack = Arrays.copyOf(keysStack, newCap);
            indexStack = Arrays.copyOf(indexStack, newCap);
        }
    }

    private void ensurePathCapacity(int min) {
        if (min > path.length) {
            path = Arrays.copyOf(path, Math.max(min, path.length * 2));
        }
    }
}

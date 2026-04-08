package whelk.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Iterative depth-first traversal of JSON-LD structures.
class DFS {
    private static final int INITIAL_CAPACITY = 32;

    private DocumentUtil.Visitor visitor;
    private List<DocumentUtil.Operation> operations;

    private Object[] path = new Object[INITIAL_CAPACITY];
    private int pathSize = 0;

    private Object[] frameContainer = new Object[INITIAL_CAPACITY]; // Map or List
    private Object[][] frameKeys = new Object[INITIAL_CAPACITY][];  // Map keys (null for Lists)
    private int[] frameIndex = new int[INITIAL_CAPACITY];
    private int[] frameSize = new int[INITIAL_CAPACITY];
    private int frameTop = 0;

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
        frameTop = 0;

        visit(obj);
        pushFrame(obj);

        while (frameTop > 0) {
            int f = frameTop - 1;

            if (frameIndex[f] >= frameSize[f]) {
                // pop frame
                frameTop--;
                if (frameTop > 0) { // root has no path
                    pathSize--;
                    path[pathSize] = null;
                }
                frameContainer[frameTop] = null;
                frameKeys[frameTop] = null;
                continue;
            }

            Object key;
            Object value;

            if (frameKeys[f] != null) { // Map
                key = frameKeys[f][frameIndex[f]];
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) frameContainer[f];
                value = map.get(key);
            } else { // List
                int idx = frameIndex[f];
                key = idx;
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) frameContainer[f];
                value = list.get(idx);
            }
            frameIndex[f]++;

            ensurePathCapacity(pathSize + 1);
            path[pathSize++] = key;

            visit(value);

            if (value instanceof Map || value instanceof List) {
                pushFrame(value);
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

    private void pushFrame(Object container) {
        ensureFrameCapacity(frameTop + 1);
        frameContainer[frameTop] = container;
        frameIndex[frameTop] = 0;

        if (container instanceof Map<?, ?> map) {
            frameKeys[frameTop] = map.keySet().toArray();
            frameSize[frameTop] = map.size();
            frameTop++;
        } else if (container instanceof List<?> list) {
            frameKeys[frameTop] = null;
            frameSize[frameTop] = list.size();
            frameTop++;
        }
    }

    private void ensureFrameCapacity(int minCapacity) {
        if (minCapacity > frameContainer.length) {
            int newCap = Math.max(minCapacity, frameContainer.length * 2);
            frameContainer = Arrays.copyOf(frameContainer, newCap);
            frameKeys = Arrays.copyOf(frameKeys, newCap);
            frameIndex = Arrays.copyOf(frameIndex, newCap);
            frameSize = Arrays.copyOf(frameSize, newCap);
        }
    }

    private void ensurePathCapacity(int minCapacity) {
        if (minCapacity > path.length) {
            path = Arrays.copyOf(path, Math.max(minCapacity, path.length * 2));
        }
    }
}

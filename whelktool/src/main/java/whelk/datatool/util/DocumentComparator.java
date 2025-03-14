package whelk.datatool.util;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

import static whelk.JsonLd.asList;

public class DocumentComparator {
    private static final Comparator<Object> BY_HASH = (o1, o2) -> o2.hashCode() - o1.hashCode();

    private final Function<Object, Boolean> isOrderedList;

    public DocumentComparator() {
        this(o -> "termComponentList".equals(o));
    }

    public DocumentComparator(Function<Object, Boolean> isOrderedList) {
        this.isOrderedList = Preconditions.checkNotNull(isOrderedList);
    }

    public boolean isEqual(Map<?, ?> a, Map<?, ?> b) {
        if (a == null || b == null || a.size() != b.size()) {
            return false;
        }
        return isEqual(a, b, this::isEqual);
    }

    public boolean isEqual(Map<?, ?> a, Map<?, ?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        for (Object key : a.keySet()) {
            if (!isEqual(a.get(key), b.get(key), key, mapCompareFunc)) {
                return false;
            }
        }
        return true;
    }

    private boolean isEqual(Object a, Object b, Object key, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        if (a == null || b == null) {
            return false;
        } else if (a.getClass() != b.getClass()) {
            return (isSingleItemList(a) && isEqual(((List<?>) a).get(0), b, key, mapCompareFunc))
                    || (isSingleItemList(b) && isEqual(a, ((List<?>) b).get(0), key, mapCompareFunc));
        } else if (a instanceof Map) {
            return mapCompareFunc.apply((Map<?, ?>) a, (Map<?, ?>) b);
        } else if (a instanceof List) {
            if (isOrderedList.apply(key)) {
                return isEqualOrdered((List<?>) a, (List<?>) b, mapCompareFunc);
            } else {
                return isEqualUnordered((List<?>) a, (List<?>) b, mapCompareFunc);
            }
        } else {
            return a.equals(b);
        }
    }

    private boolean isSingleItem(Object o) {
        return asList(o).size() == 1;
    }

    private boolean isSingleItemList(Object o) {
        return o instanceof List && ((List<?>) o).size() == 1;
    }

    private boolean isEqualOrdered(List<?> a, List<?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!isEqual(a.get(i), b.get(i), null, mapCompareFunc)) {
                return false;
            }
        }
        return true;
    }

    private boolean isEqualUnordered(List<?> a, List<?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        if (a.size() != b.size()) {
            return false;
        }

        a = new ArrayList<>(a);
        b = new ArrayList<>(b);

        a.sort(BY_HASH);
        b.sort(BY_HASH);

        List<Integer> taken = new ArrayList<>(a.size());
        nextA:
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if (!taken.contains(j) && isEqual(a.get(i), b.get(j), null, mapCompareFunc)) {
                    taken.add(j);
                    continue nextA;
                }
            }
            return false;
        }

        return true;
    }

    public boolean isSubset(Map<?, ?> a, Map<?, ?> b) {
        if (a == null || b == null || a.size() > b.size()) {
            return false;
        }
        return isSubset(a, b, this::isSubset);
    }

    public boolean isSubset(Map<?, ?> a, Map<?, ?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        for (Object key : a.keySet()) {
            if (!isSubset(a.get(key), b.get(key), key, mapCompareFunc)) {
                return false;
            }
        }
        return true;
    }

    private boolean isSubset(Object a, Object b, Object key, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        if (a == null || b == null) {
            return false;
        } else if (a.getClass() != b.getClass()) {
            return (isSingleItem(a) && b instanceof List && isSubset(asList(a), b, key, mapCompareFunc))
                    || (isSingleItemList(a) && isSubset(((List<?>) a).getFirst(), b, key, mapCompareFunc))
                    || (isSingleItemList(b) && isSubset(a, ((List<?>) b).getFirst(), key, mapCompareFunc));
        } else if (a instanceof Map) {
            return mapCompareFunc.apply((Map<?, ?>) a, (Map<?, ?>) b);
        } else if (a instanceof List) {
            if (isOrderedList.apply(key)) {
                return isOrderedSubset((List<?>) a, (List<?>) b, mapCompareFunc);
            } else {
                return isUnorderedSubset((List<?>) a, (List<?>) b, mapCompareFunc);
            }
        } else {
            return a.equals(b);
        }
    }

    private boolean isOrderedSubset(List<?> a, List<?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        if (a.size() > b.size()) {
            return false;
        }
        int ixB = 0;
        for (int ixA = 0; ixA < a.size(); ixA++) {
            if (ixB == b.size()) {
                return false;
            }

            while (!isSubset(a.get(ixA), b.get(ixB++), null, mapCompareFunc)) {
                if (ixB == b.size()) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isUnorderedSubset(List<?> a, List<?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
        return new UnorderedListComparator(a, b, mapCompareFunc).isSubset();
    }

    private class UnorderedListComparator {
        List a;
        List b;

        BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc;

        Stack<Integer> stack;
        Stack<Integer> matched;
        boolean anyMatch;
        Boolean[][] cache;

        UnorderedListComparator(List<?> a, List<?> b, BiFunction<Map<?, ?>, Map<?, ?>, Boolean> mapCompareFunc) {
            this.a = a;
            this.b = b;
            this.mapCompareFunc = mapCompareFunc;
            cache = new Boolean[a.size()][b.size()];
        }

        boolean isSubset() {
            // since elements in 'a' might be subsets of more than one element
            // in 'b' we must try different ways of matching elements
            stack = new Stack<>();
            matched = new Stack<>();

            nextA();
            while (stack.size() > 0) {
                boolean match = isSubset(ixA(), ixB());
                nextB();
                if (match) {
                    anyMatch = true;
                    if (!matched.contains(ixB())) {
                        matched.push(ixB());
                        if (matched.size() == a.size()) {
                            return true;
                        }
                        nextA();
                    }
                }

                while (ixB() == b.size()) {
                    if (!anyMatch) {
                        return false;
                    }
                    previousA();
                }
            }

            return false;
        }

        private boolean isSubset(int ixA, int ixB) {
            if (cache[ixA][ixB] == null) {
                cache[ixA][ixB] = DocumentComparator.this.isSubset(a.get(ixA), b.get(ixB), null, mapCompareFunc);
            }

            return cache[ixA][ixB];
        }

        private void previousA() {
            stack.pop();
            if (matched.size() > 0) {
                matched.pop();
            }
        }

        private void nextA() {
            stack.push(0);
            anyMatch = false;
        }

        private void nextB() {
            stack.push(stack.pop() + 1);
        }

        private int ixA() {
            return stack.size() - 1;
        }

        private int ixB() {
            return stack.size() > 0 ? stack.peek() : -1;
        }
    }
}

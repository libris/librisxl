package trld;

import java.util.*;
import java.util.function.Function;

public class Builtins {

    public static List sorted(Collection items) {
        return sorted(items, false);
    }

    public static List sorted(Collection items, boolean reversed) {
        return sorted(items, null, false);
    }

    public static List sorted(Collection items, Function<Object, Comparable> getKey, boolean reversed) {
        List result = new ArrayList(items.size());
        result.addAll(items);
        Comparator cmp = makeComparator(getKey, reversed);
        Collections.sort(result, cmp);
        return result;
    }

    public static Comparator makeComparator(Function<Object, Comparable> getKey, boolean reversed) {
        Comparator cmp = null;
        if (getKey != null) {
            cmp = (a, b) -> getKey.apply(a).compareTo(getKey.apply(b));
        }
        if (reversed) {
            cmp = Collections.reverseOrder(cmp);
        }
        return cmp;
    }

    public static Map mapOf(Object ...pairs) {
        Map result = new HashMap<>(pairs.length);
        int i = 0;
        Object key = null;
        for (Object item : pairs) {
            if (++i % 2 == 0) {
                result.put(key, item);
            } else {
                key = item;
            }
        }
        return result;
    }
}

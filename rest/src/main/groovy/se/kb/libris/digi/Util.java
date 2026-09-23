package se.kb.libris.digi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Util {
    static final String JSONLD = "application/ld+json";

    /**
     * Wraps in a list, treating Groovy-falsy values as empty, since the original
     * Groovy implementation relied on that.
     */
    static List<Object> asList(Object o) {
        if (o instanceof List) {
            return new ArrayList<>((List<?>) o);
        }
        return isTruthy(o) ? new ArrayList<>(List.of(o)) : new ArrayList<>();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : new LinkedHashMap<>();
    }

    static Set<Object> asSet(Object o) {
        return new LinkedHashSet<>(asList(o));
    }

    static Object getAtPath(Object item, List<Object> path) {
        return getAtPath(item, path, null);
    }

    static Object getAtPath(Object item, List<Object> path, Object defaultTo) {
        if (!isTruthy(item)) {
            return defaultTo;
        }

        for (int i = 0; i < path.size(); i++) {
            Object p = path.get(i);

            if ("*".equals(p) && item instanceof Collection) {
                List<Object> rest = path.subList(i + 1, path.size());
                List<Object> collected = new ArrayList<>();
                for (Object it : (Collection<?>) item) {
                    flattenInto(getAtPath(it, rest, new ArrayList<>()), collected);
                }
                return collected;
            }

            Object next = get(item, p);
            if (next != null) {
                item = next;
            } else {
                return defaultTo;
            }
        }
        return item;
    }

    private static Object get(Object item, Object key) {
        if (item instanceof Map) {
            return ((Map<?, ?>) item).get(key);
        }
        if (item instanceof List && key instanceof Integer) {
            List<?> l = (List<?>) item;
            int i = (Integer) key;
            return i >= 0 && i < l.size() ? l.get(i) : null;
        }
        return null;
    }

    private static void flattenInto(Object o, List<Object> target) {
        if (o instanceof Collection) {
            for (Object e : (Collection<?>) o) {
                flattenInto(e, target);
            }
        } else if (o != null) {
            target.add(o);
        }
    }

    static boolean isLink(Object m) {
        return m instanceof Map
                && isTruthy(((Map<?, ?>) m).get("@id"))
                && ((Map<?, ?>) m).size() == 1;
    }

    /** Groovy truth  */
    static boolean isTruthy(Object o) {
        return switch (o) {
            case null -> false;
            case Boolean b -> b;
            case CharSequence s -> !s.isEmpty();
            case Collection<?> c -> !c.isEmpty();
            case Map<?, ?> m -> !m.isEmpty();
            case Number n -> n.doubleValue() != 0;
            default -> true;
        };
    }
}

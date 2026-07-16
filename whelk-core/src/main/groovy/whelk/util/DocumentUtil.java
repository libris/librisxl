package whelk.util;

import whelk.JsonLd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DocumentUtil {
    public final static Operation NOP = new Nop();

    public interface Visitor {
        Operation visitElement(Object value, List<Object> path);
    }

    public interface Linker {
        /**
         * Called for every blank node found in search
         *
         * @param blankNode
         * @param existingLinks List of sibling node @ids. Can be used for disambiguation.
         * @return
         */
        List<Map> link(Map blankNode, List existingLinks);

        /**
         * This is called when the blank node search encounters
         * a single string value where there would normally be a node
         */
        List<Map> link(String blank, List existingLinks);
    }

    /**
     * Traverse a JSON-LD structure in depth-first order
     *
     * @param data JSON-LD structure
     * @param visitor function to call for every value
     * @return true if data was changed
     */
    public static boolean traverse(Object data, Visitor visitor) {
        return new DFS().traverse(data, visitor);
    }

    /**
     * Search for a key in JSON-LD structure
     *
     * @param data JSON-LD structure
     * @param key
     * @param visitor function to call with value for found keys
     * @return true if obj was changed
     */
    public static boolean findKey(Object data, String key, Visitor visitor) {
        return findKey(data, List.of(key), visitor);
    }

    /**
     * Search keys in JSON-LD structure
     *
     * @param data JSON-LD structure
     * @param keys
     * @param visitor function to call with value for found keys
     * @return true if obj was changed
     */
    public static boolean findKey(Object data, Collection<String> keys, Visitor visitor) {
        Set<String> k = keys instanceof Set ? (Set<String>) keys : new HashSet<>(keys);
        return traverse(data, (value, path) -> {
            if (path != null && !path.isEmpty() && path.getLast() instanceof String key && k.contains(key)) {
                return visitor.visitElement(value, path);
            }
            return null;
        });
    }

    public static Visitor link(Linker linker) {
        return link(linker, new ArrayList<>());
    }

    public static Visitor link(Linker linker, List<Map> disambiguationNodes) {
        return (value, path) -> linkBlankNodes(value, linker, disambiguationNodes);
    }

    /**
     *
     * @param objectOrArray
     * @param linker
     * @return
     */
    public static Operation linkBlankNodes(Object objectOrArray, Linker linker) {
        return linkBlankNodes(objectOrArray, linker, new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public static Operation linkBlankNodes(Object objectOrArray, Linker linker, List<Map> disambiguationNodes) {
        return switch (objectOrArray) {
            case List<?> nodes -> linkBlankNode((List<Map>) nodes, linker, disambiguationNodes);
            case Map<?, ?> node -> linkBlankNode((Map) node, linker, disambiguationNodes);
            case String singleValue -> linkBlankNode(singleValue, linker, disambiguationNodes);
            case null, default -> NOP;
        };
    }

    public static boolean isBlank(Map node) {
        return !node.containsKey("@id");
    }

    /**
     *
     * @param item
     * @param path
     * @param defaultTo
     * @param requireListIndex
     * @return
     */
    public static Object getAtPath(Object item, Iterable<?> path) {
        return getAtPath(item, path, null);
    }

    public static Object getAtPath(Object item, Iterable<?> path, Object defaultTo) {
        return getAtPath(item, path, defaultTo, true);
    }

    public static Object getAtPath(Object item, Iterable<?> path, Object defaultTo, boolean requireListIndex) {
        if (item == null
                || (item instanceof Map<?, ?> m && m.isEmpty())
                || (item instanceof Collection<?> c && c.isEmpty())
                || (item instanceof CharSequence s && s.isEmpty())) {
            return defaultTo;
        }

        List<?> pathList = toList(path);
        for (int i = 0; i < pathList.size(); i++) {
            Object p = pathList.get(i);
            if (p instanceof JsonLdKey key) {
                p = key.key();
            }
            if (p instanceof Enum<?> e) {
                p = e.toString();
            }

            if (p.equals("*")) {
                if (item instanceof Collection<?> collection) {
                    List<Object> result = new ArrayList<>();
                    for (Object o : collection) {
                        flattenInto(result, getAtPath(o, pathList.subList(i + 1, pathList.size()), new ArrayList<>()));
                    }
                    return result;
                } else {
                    return new ArrayList<>();
                }
            } else if (getAt(item, p) != null) {
                item = getAt(item, p);
            } else if (item instanceof Collection<?> collection && !requireListIndex) {
                List<Object> result = new ArrayList<>();
                for (Object o : collection) {
                    flattenInto(result, getAtPath(o, pathList.subList(i, pathList.size()), new ArrayList<>(), requireListIndex));
                }
                return result;
            } else {
                return defaultTo;
            }
        }
        return item;
    }

    private static Object getAt(Object item, Object p) {
        if (item instanceof Map<?, ?> map) {
            return map.get(p);
        }
        if (item instanceof List<?> list && p instanceof Integer i) {
            int index = i < 0 ? list.size() + i : i;
            return index >= 0 && index < list.size() ? list.get(index) : null;
        }
        return null;
    }

    private static List<?> toList(Iterable<?> path) {
        if (path instanceof List<?> list) {
            return list;
        }
        List<Object> result = new ArrayList<>();
        path.forEach(result::add);
        return result;
    }

    private static void flattenInto(List<Object> result, Object value) {
        if (value instanceof Collection<?> collection) {
            for (Object o : collection) {
                flattenInto(result, o);
            }
        } else {
            result.add(value);
        }
    }

    private static Operation linkBlankNode(List<Map> nodes, Linker linker, List<Map> disambiguationNodes) {
        if (nodes.stream().noneMatch(DocumentUtil::isBlank)) {
            return NOP;
        }

        List<Object> existingLinks = collectIris(nodes);
        List<Object> result = new ArrayList<>();

        for (Map node : nodes) {
            if (isDefective(node)) {
                continue; // remove node
            }
            List<Map> newLinked = null;
            if (isBlank(node)) {
                newLinked = linker.link(node, existingLinks);
                if (newLinked == null || newLinked.isEmpty()) {
                    newLinked = linker.link(node, collectIris(disambiguationNodes));
                }
            }
            if (newLinked != null && !newLinked.isEmpty()) {
                for (Map l : newLinked) {
                    if (!existingLinks.contains(l.get("@id"))) {
                        result.add(l);
                    }
                }
            } else {
                result.add(node);
            }
        }

        if (!nodes.equals(result)) {
            return new Replace(result);
        } else {
            return NOP;
        }
    }

    private static Operation linkBlankNode(Map node, Linker linker, List<Map> disambiguationNodes) {
        if (isDefective(node)) {
            return new Remove();
        }
        if (!isBlank(node)) {
            return NOP;
        }

        return toOperation(linker.link(node, collectIris(disambiguationNodes)));
    }

    private static Operation linkBlankNode(String singleValue, Linker linker, List<Map> disambiguationNodes) {
        return toOperation(linker.link(singleValue, collectIris(disambiguationNodes)));
    }

    private static Operation toOperation(List<Map> replacement) {
        return replacement != null && !replacement.isEmpty()
                ? replacement.size() > 1 ? new Replace(replacement) : new Replace(replacement.get(0))
                : NOP;
    }

    private static boolean isDefective(Map node) {
        return node.size() == 0 || (node.size() == 1 && node.containsKey(JsonLd.TYPE_KEY));
    }

    private static List<Object> collectIris(List<Map> nodes) {
        List<Object> iris = new ArrayList<>();
        for (Map node : nodes) {
            if (!isBlank(node)) {
                iris.add(node.get("@id"));
            }
        }
        return iris;
    }

    public static abstract class Operation {
        List<Object> path;

        protected abstract void perform(Object obj);

        protected Operation setPath(List<Object> path) {
            this.path = new ArrayList<>(path);
            return this;
        }

        protected ParentAndKey parentAndKey(Object obj) {
            List<Object> p = new ArrayList<>(path);
            while (p.size() > 1) {
                obj = getAt(obj, p.remove(0));
                if (obj == null) {
                    // already gone
                    return new ParentAndKey(null, null);
                }
            }
            return new ParentAndKey(obj, p.get(0));
        }

        protected record ParentAndKey(Object parent, Object key) {}
    }

    public static class Nop extends Operation {
        @Override
        protected void perform(Object obj) {}

        @Override
        protected Operation setPath(List<Object> path) { return this; }
    }

    public static class Remove extends Operation {
        @Override
        protected void perform(Object obj) {
            var pk = parentAndKey(obj);
            Object parent = pk.parent();
            if (parent == null || isEmptyContainer(parent)) {
                return;
            }

            if (parent instanceof Map<?, ?> map) {
                map.remove(pk.key());
            } else if (parent instanceof List<?> list && pk.key() instanceof Integer i) {
                list.remove((int) i);
            }
            if (isEmptyContainer(parent) && path.size() > 1) {
                new Remove().setPath(path.subList(0, path.size() - 1)).perform(obj);
            }
        }

        private static boolean isEmptyContainer(Object o) {
            return (o instanceof Map<?, ?> m && m.isEmpty()) || (o instanceof Collection<?> c && c.isEmpty());
        }
    }

    public static class Replace extends Operation {
        Object with;

        public Replace(Object with) {
            this.with = with;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void perform(Object obj) {
            var pk = parentAndKey(obj);
            Object parent = pk.parent();
            if (parent != null) {
                if (parent instanceof Map) {
                    ((Map<Object, Object>) parent).put(pk.key(), with);
                } else if (parent instanceof List && pk.key() instanceof Integer i) {
                    ((List<Object>) parent).set(i, with);
                }
            }
        }
    }
}

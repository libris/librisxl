package whelk.converter;

import java.util.*;

public class EmbedBlanks {

    public static void embedBlanks(Object data) {
        if (data instanceof Map dataMap && dataMap.containsKey("@graph")) {
            Map<String, Map> blankIndex = new HashMap<>();
            Map<String, List<Map>> references = new HashMap<>();
            Map<String, Set<String>> reachableFrom = new HashMap<>();
            List<Object> topLevel = new ArrayList<>();

            var graphValue = dataMap.get("@graph");
            List objects = null;
            if (graphValue instanceof List list) {
                objects = list;
            } else {
                objects = new ArrayList();
                objects.add(graphValue);
            }

            for (Object o : objects) {
                if (o instanceof Map node) {
                    var reachable = new HashSet<String>();
                    collectBlankReferences(node, references, reachable);
                    if (node.containsKey("@id")) {
                        var id = (String) node.get("@id");
                        if (id.startsWith("_:")) {
                            blankIndex.put(id, node);
                            reachableFrom.put(id, reachable);
                        } else {
                            topLevel.add(node);
                        }
                    } else {
                        topLevel.add(node);
                    }
                } else {
                    topLevel.add(o);
                }
            }

            var keySet = blankIndex.keySet();
            var keys = new ArrayList<String>(keySet);
            Collections.sort(keys);
            for (var id : keys) {
                var node = blankIndex.get(id);

                var embed = true;
                if (!references.containsKey(id)) {
                    embed = false;
                } else {
                    if (isCyclic(id, id, new HashSet<String>(), reachableFrom)) {
                        embed = false;
                    }
                }

                var refs = references.get(id);
                if (refs == null || refs.size() != 1) {
                    embed = false;
                }

                if (!embed) {
                    topLevel.add(node);
                    continue;
                }

                var ref = refs.get(0);
                ref.putAll(node);
                ref.remove("@id");
            }

          dataMap.put("@graph", topLevel);
        }
    }

    protected static void collectBlankReferences(Map node, Map<String, List<Map>> references, Set<String> reachable) {
        var id = (String) node.get("@id");
        for (var key : node.keySet()) {
            var value = node.get(key);
            if (value instanceof Map childNode) {
                addBlankReference(childNode, references, reachable);
                collectBlankReferences(childNode, references, reachable);
            } else if (value instanceof List list) {
              for (var item : list) {
                if (item instanceof Map itemNode) {
                  addBlankReference(itemNode, references, reachable);
                  collectBlankReferences(itemNode, references, reachable);
                }
              }
            }
        }
    }

    protected static void addBlankReference(Map ref, Map<String, List<Map>> references, Set<String> reachable) {
        if (ref.containsKey("@id")) {
            var id = (String) ref.get("@id");
            if (id.startsWith("_:") && ref.size() == 1) {
                var refs = references.get(id);
                if (refs == null) {
                    refs = new ArrayList<>();
                    references.put(id, refs);
                }
                refs.add(ref);
                if (id != null) {
                    reachable.add(id);
                }
            }
        }
    }

    protected static boolean isCyclic(String id, String via, Set<String> seen, Map<String, Set<String>> reachableFrom) {
        if (!seen.add(via)) {
            return false;
        }
        var reachable = reachableFrom.get(via);
        if (reachable == null) {
            return false;
        }
        if (reachable.contains(id)) {
            return true;
        }
        for (String other : reachable) {
            if(isCyclic(id, other, seen, reachableFrom)) {
                return true;
            }
        }
        return false;
    }

}

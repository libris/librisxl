package whelk.converter;

import java.util.*;

public class EmbedBlanks {

    public static Object embedBlanks(Object data) {
        if (data instanceof Map dataMap && dataMap.containsKey("@graph")) {
            Map<String, Map> blankIndex = new HashMap<>();
            Map<String, List<Map>> references = new HashMap<>();
            List<Map> named = new ArrayList<>();

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
                    collectBlankReferences(node, references);
                    if (node.containsKey("@id")) {
                        var id = (String) node.get("@id");
                        if (id.startsWith("_:")) {
                            blankIndex.put(id, node);
                        } else {
                            named.add(node);
                        }
                    }
                }
            }

            for (var id : references.keySet()) {
                var node = blankIndex.get(id);
                if (node != null) {
                    var refs = references.get(id);
                    if (refs.size() == 1) {
                        var ref = refs.get(0);;
                        ref.putAll(node);
                        ref.remove("@id");
                    } else {
                        named.add(node);
                    }
                }
            }

          var graphNode = new HashMap();
          graphNode.put("@graph", named);
          return graphNode;
        } else {
          return data;
        }
    }

    protected static void collectBlankReferences(Map node, Map<String, List<Map>> references) {
        addBlankReference(node, references);
        for (var key : node.keySet()) {
            var value = node.get(key);
            if (value instanceof Map childNode) {
                collectBlankReferences(childNode, references);
            } else if (value instanceof List list) {
              for (var item : list) {
                if (item instanceof Map itemNode) {
                  collectBlankReferences(itemNode, references);
                }
              }
            }
        }
    }

    protected static void addBlankReference(Map ref, Map<String, List<Map>> references) {
        if (ref.containsKey("@id")) {
            var id = (String) ref.get("@id");
            if (id.startsWith("_:") && ref.size() == 1) {
                var refs = references.get(id);
                if (refs == null) {
                    refs = new ArrayList<>();
                    references.put(id, refs);
                }
                refs.add(ref);
            }
        }
    }

}

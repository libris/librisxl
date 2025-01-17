package whelk.search2;

import whelk.JsonLd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.getAtPath;

public class FacetTree {

    private final JsonLd jsonLd;

    public FacetTree(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
    }

    public List<Map<String, Object>> sortObservationsAsTree(List<Map<String, Object>> observations) {
        List<Map<String, Object>> tree = new ArrayList<>();
        Queue<Map<String, Object>> queue = new ConcurrentLinkedQueue<>();

        observations.forEach(observation -> {
            var parent = findParent(observation, observations);

            if (parent == null) {
                tree.add(observation);
                queue.add(observation);
            }
        });

        while (!queue.isEmpty()) {
            var observation = queue.remove();
            var children = findChildren(observation, observations);
            if (!children.isEmpty()) {
                queue.addAll(children);
                observation.put("_children", children);
            }
        }
        return List.copyOf(tree);
    }

    private Map<String, Object> findParent(Map<String, Object> observation, List<Map<String, Object>> observations) {
        return observations.stream()
                .filter(o -> isSubClass(observation, o))
                .findFirst().orElse(null);
    }

    private List<Map<String, Object>> findChildren(Map<String, Object> observation, List<Map<String, Object>> observations) {
        return observations.stream()
                .filter(o -> isSubClass(o, observation))
                .collect(Collectors.toList());
    }

    private boolean isSubClass(Map<String, Object> obsA, Map<String, Object> obsB) {
        String idA = get(obsA, List.of("object", "@id"), "");
        String idB = get(obsB, List.of("object", "@id"), "");
        return !idA.equals(idB) && jsonLd.isSubClassOf(idA, idB);
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(Object m, List<Object> path, T defaultTo) {
        return (T) getAtPath(m, path, defaultTo);
    }
}

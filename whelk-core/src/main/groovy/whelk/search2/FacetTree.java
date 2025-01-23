package whelk.search2;

import whelk.JsonLd;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.getAtPath;

public class FacetTree {

    private final JsonLd jsonLd;
    private List<String> observationsAsTypeKeys = Collections.emptyList();

    public FacetTree(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
    }

    public List<Map<String, Object>> sortObservationsAsTree(List<Map<String, Object>> observations) {
        List<Map<String, Object>> tree = new ArrayList<>();
        Queue<Map<String, Object>> queue = new ConcurrentLinkedQueue<>();
        List<String> intermediateClasses = new ArrayList<>();

        observationsAsTypeKeys = observations.stream()
                .map(o -> jsonLd.toTermKey(get(o, List.of("object", "@id"), "")))
                .toList();

        observations.forEach(observation -> {
            if (isRootNode(observation)) {
                tree.add(observation);
                queue.add(observation);
            } else {
                intermediateClasses.addAll(getAbsentSuperClasses(observation));
            }
        });

        // Create empty observations for intermediate classes with 0 totalHits
        observations.addAll(intermediateClasses.stream().map(this::createFakeObservation).toList());

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

    private Map<String, Object> createFakeObservation(String termKey) {
        Map<String, Object> fakeObservation = new LinkedHashMap<>();
        String termId = jsonLd.toTermId(termKey);
        var fakeObject = Map.of(JsonLd.ID_KEY, termId);
        fakeObservation.put("totalItems", 0);
        fakeObservation.put("view", Map.of(JsonLd.ID_KEY, "fake"));
        fakeObservation.put("object", fakeObject);
        return fakeObservation;
    }

    private List<String> getAbsentSuperClasses(Map<String, Object> observation) {
        String typeKey = jsonLd.toTermKey(get(observation, List.of("object", "@id"), ""));
        List<String> allSuperClasses = jsonLd.getSuperClasses(typeKey);

        return allSuperClasses.stream()
                .takeWhile(s -> !observationsAsTypeKeys.contains(s))
                .toList();
    }

    private boolean hasParentInObservations(Map<String, Object> observation) {
        String typeKey = jsonLd.toTermKey(get(observation, List.of("object", "@id"), ""));
        List<String> allSuperClasses = jsonLd.getSuperClasses(typeKey);

        return allSuperClasses.stream()
                .anyMatch(s -> observationsAsTypeKeys.contains(s));
    }

    private boolean isRootNode(Map<String, Object> observation) {
        return !hasParentInObservations(observation);
    }

    private List<Map<String, Object>> findChildren(Map<String, Object> observation, List<Map<String, Object>> observations) {
        return observations.stream()
                .filter(o -> isDirectSubclass(o, observation))
                .collect(Collectors.toList());
    }

    private boolean isDirectSubclass(Map<String, Object> obsA, Map<String, Object> obsB) {
        String idA = jsonLd.toTermKey(get(obsA, List.of("object", "@id"), ""));
        String idB = jsonLd.toTermKey(get(obsB, List.of("object", "@id"), ""));
        List<String> directSubclasses = jsonLd.getDirectSubclasses(idB);
        return directSubclasses.contains(idA);
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(Object m, List<Object> path, T defaultTo) {
        return (T) getAtPath(m, path, defaultTo);
    }
}

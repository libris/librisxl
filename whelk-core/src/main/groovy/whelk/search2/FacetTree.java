package whelk.search2;

import whelk.JsonLd;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.getAtPath;

public class FacetTree {

    private final JsonLd jsonLd;
    private Map<String, Map<String, Object>> keyToObservation = new HashMap<>();

    public FacetTree(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
    }

    public List<Map<String, Object>> sortObservationsAsTree(List<Map<String, Object>> observations) {
        List<Map<String, Object>> tree = new ArrayList<>();
        Queue<Map<String, Object>> queue = new ConcurrentLinkedQueue<>();
        Set<String> intermediateClasses = new HashSet<>();

        keyToObservation = observations.stream()
                .collect(Collectors.toMap(o -> jsonLd.toTermKey(get(o, List.of("object", "@id"), "")), Function.identity()));

        List<String> rootCandidates = keyToObservation.keySet().stream().filter(this::isRootNode).toList();
        String rootKey = "";

        if (rootCandidates.size() == 1) {
            rootKey = rootCandidates.getFirst();
            var root = keyToObservation.get(rootKey);
            tree.add(root);
            queue.add(root);
        } else {
            Optional<String> first = keyToObservation.keySet().stream().findFirst();
            if (first.isPresent()) {
                Optional<String> rootKeyOpt = getAbsentRoot(first.get());
                if (rootKeyOpt.isPresent()) {
                    rootKey = rootKeyOpt.get();
                    var root = createFakeObservation(rootKey);
                    observations.add(root);
                    tree.add(root);
                    queue.add(root);
                }
            }
        }

        for (String typeKey : keyToObservation.keySet()) {
            if (!typeKey.equals(rootKey)) {
                intermediateClasses.addAll(getIntermediateClassesFor(typeKey));
            }
        }

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
        if (termId == null) {
            // TODO: investigate!!
            // Happens when observations are "" and "Dataset".
            return new HashMap<>();
        }
        var fakeObject = Map.of(JsonLd.ID_KEY, termId);
        fakeObservation.put("totalItems", 0);
        fakeObservation.put("view", Map.of(JsonLd.ID_KEY, "fake"));
        fakeObservation.put("object", fakeObject);
        return fakeObservation;
    }

    private List<String> getIntermediateClassesFor(String typeKey) {
        return getAbsentSuperClasses(typeKey);
    }

    private List<String> getAbsentSuperClasses(String typeKey) {
        List<String> allSuperClasses = jsonLd.getSuperClasses(typeKey);

        return allSuperClasses.stream()
                .takeWhile(s -> !keyToObservation.containsKey(s))
                .toList();
    }

    private Optional<String> getAbsentRoot(String typeKey) {
        List<String> allSuperClasses = jsonLd.getSuperClasses(typeKey);
        return allSuperClasses.stream()
                .filter(this::subClassesContainsAllObservations)
                .findFirst();
    }

    private boolean subClassesContainsAllObservations(String c) {
        Set<String> subClasses = jsonLd.getSubClasses(c);
        return subClasses.containsAll(keyToObservation.keySet());
    }

    private boolean hasParentInObservations(String typeKey) {
        List<String> allSuperClasses = jsonLd.getSuperClasses(typeKey);

        return allSuperClasses.stream()
                .anyMatch(s -> keyToObservation.containsKey(s));
    }

    private boolean isRootNode(String typeKey) {
        return !hasParentInObservations(typeKey);
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

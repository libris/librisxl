package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.QueryUtil;

import java.util.*;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.nestedWrap;


public sealed abstract class Group implements Node permits And, Or {
    @Override
    public abstract List<Node> children();

    abstract Group newInstance(List<Node> children);

    abstract String delimiter();

    abstract String key();

    abstract Map<String, Object> wrap(List<Map<String, Object>> esChildren);

    @Override
    public abstract boolean equals(Object o);

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), new HashSet<>(children()));
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        Map<String, List<Node>> nestedGroups = getNestedGroups(esSettings.mappings());
        return nestedGroups.isEmpty() ? wrap(childrenToEs(esSettings)) : toEsNested(nestedGroups, esSettings);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), mapToMap(c -> c.toSearchMapping(makeUpLink)));
        m.put("up", makeUpLink.apply(this));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = doMapToString(n -> n.toQueryString(false))
                .collect(Collectors.joining(delimiter()));
        return topLevel ? s : QueryUtil.parenthesize(s);
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    List<Node> flattenChildren(List<Node> children) {
        List<Node> flattened = new ArrayList<>();
        for (Node child : children) {
            if (child instanceof Group g && g.getClass() == this.getClass()) {
                g.children().stream().filter(c -> !flattened.contains(c) && !children.contains(c)).forEach(flattened::add);
            } else {
                flattened.add(child);
            }
        }
        return flattened;
    }

    Node filterAndReinstantiate(Predicate<Node> p) {
        return mapFilterAndReinstantiate(Function.identity(), p);
    }

    Group mapAndReinstantiate(Function<Node, Node> mapper) {
        return newInstance(mapToNode(mapper));
    }

    Node mapFilterAndReinstantiate(Function<Node, Node> mapper, Predicate<Node> p) {
        List<Node> newChildren = mapToNodeAndFilter(mapper, p);
        return switch (newChildren.size()) {
            case 0 -> null;
            case 1 -> newChildren.getFirst();
            default -> newInstance(newChildren);
        };
    }

    Node reduce(JsonLd jsonLd, BiFunction<Node, Node, Optional<Node>> pick) {
        List<Node> reduced = new ArrayList<>();
        children().stream().map(child -> child.reduce(jsonLd))
                .forEach(child -> {
                    for (int i = 0; i < reduced.size(); i++) {
                        Optional<Node> picked = pick.apply(child, reduced.get(i));
                        if (picked.isPresent()) {
                            reduced.set(i, picked.get());
                            return;
                        }
                    }
                    reduced.add(child);
                });
        return reduced.size() == 1 ? reduced.getFirst() : newInstance(reduced);
    }

    private Optional<Node> pick(Node a, Node b, JsonLd jsonLd) {
        if (a.implies(b, jsonLd)) {
            return Optional.of(a);
        }
        if (b.implies(a, jsonLd)) {
            return Optional.of(b);
        }
        return Optional.empty();
    }

    private Map<String, Object> toEsNested(Map<String, List<Node>> nestedGroups, ESSettings esSettings) {
        List<Map<String, Object>> esChildren = new ArrayList<>();
        List<Node> nonNested = new ArrayList<>(children());

        nestedGroups.forEach((nestedStem, group) -> {
            var es = nestedWrap(nestedStem, wrap(group.stream().map(n -> toCoreEsQuery(n, esSettings)).toList()));
            esChildren.add(es);
            nonNested.removeAll(group);
        });

        for (Node n : nonNested) {
            esChildren.add(n.toEs(esSettings));
        }

        return esChildren.size() == 1 ? esChildren.getFirst() : wrap(esChildren);
    }

    private Map<String, List<Node>> getNestedGroups(EsMappings esMappings) {
        Map<String, List<Node>> nestedGroups = new HashMap<>();

        children().stream().collect(Collectors.groupingBy(node -> getEsNestedStem(node, esMappings)))
                .forEach((nestedStem, group) -> {
                    // At least two different paths sharing the same nested stem
                    if (nestedStem.isPresent() && group.size() > 1) {
                        nestedGroups.put(nestedStem.get(), group);
                    }
                });

        return nestedGroups;
    }

    private Optional<String> getEsNestedStem(Node node, EsMappings esMappings) {
        if (node instanceof PathValue pv) {
            return pv.path().getEsNestedStem(esMappings);
        }
        if (node instanceof Group g) {
            var groupedByNestedStem = g.children().stream().collect(Collectors.groupingBy(n -> getEsNestedStem(n, esMappings)));
            if (groupedByNestedStem.size() == 1) {
                return groupedByNestedStem.keySet().iterator().next();
            }
        }
        return Optional.empty();
    }

    private Map<String, Object> toCoreEsQuery(Node node, ESSettings esSettings) {
        return switch (node) {
            case PathValue pv -> pv.getCoreEsQuery(esSettings);
            case Group g -> g.wrap(g.childrenToCoreEs(esSettings));
            default -> node.toEs(esSettings);
        };
    }

    private List<Map<String, Object>> childrenToEs(ESSettings esSettings) {
        return mapToMap(n -> n.toEs(esSettings));
    }

    private List<Map<String, Object>> childrenToCoreEs(ESSettings esSettings) {
        return mapToMap(n -> toCoreEsQuery(n, esSettings));
    }

    private List<Node> mapToNode(Function<Node, Node> mapper) {
        return mapToNodeAndFilter(mapper, x -> true);
    }

    private List<Node> mapToNodeAndFilter(Function<Node, Node> mapper, Predicate<Node> filter) {
        return children().stream().map(mapper).filter(filter).collect(Collectors.toList());
    }

    private List<Map<String, Object>> mapToMap(Function<Node, Map<String, Object>> mapper) {
        return mapToMapAndFilter(mapper, x -> true);
    }

    private List<Map<String, Object>> mapToMapAndFilter(Function<Node, Map<String, Object>> mapper,
                                                        Predicate<Map<String, Object>> filter) {
        return children().stream().map(mapper).filter(filter).collect(Collectors.toList());
    }

    private Stream<String> doMapToString(Function<Node, String> mapper) {
        return children().stream().map(mapper);
    }
}

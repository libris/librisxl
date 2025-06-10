package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.EsBoost;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;

import java.util.*;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.nestedWrap;


public sealed abstract class Group implements Node permits And, Or {
    @Override
    public abstract List<Node> children();

    abstract Group newInstance(List<Node> children);

    abstract String delimiter();

    abstract String key();

    abstract Map<String, Object> wrap(List<Map<String, Object>> esChildren);

    abstract List<String> collectRulingTypes();

    abstract boolean implies(Node a, Node b, BiFunction<Node, Node, Boolean> condition);

    @Override
    public abstract boolean equals(Object o);

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), new HashSet<>(children()));
    }

    @Override
    public Map<String, Object> toEs(EsMappings esMappings, EsBoost.Config boostConfig) {
        Map<String, List<PathValue>> nestedGroups = getNestedGroups(esMappings);
        return nestedGroups.isEmpty() ? wrap(childrenToEs(esMappings, boostConfig)) : toEsNested(nestedGroups, esMappings, boostConfig);
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), mapToMap(c -> c.toSearchMapping(qt, queryParams)));
        m.put("up", makeUpLink(qt, this, queryParams));
        return m;
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        Node reduced = reduceTypes(jsonLd);
        if (reduced instanceof Group g) {
            rulingTypes = Stream.concat(g.collectRulingTypes().stream(), rulingTypes.stream())
                    .collect(Collectors.toSet());
            return g.expandChildren(jsonLd, rulingTypes);
        }
        return reduced.expand(jsonLd, rulingTypes);
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

    @Override
    public Node reduceTypes(JsonLd jsonLd) {
        BiFunction<Node, Node, Boolean> hasMoreSpecificTypeThan = (a, b) -> a.isTypeNode()
                && b.isTypeNode()
                && jsonLd.isSubClassOf(((VocabTerm) (((PathValue) a).value())).key(), ((VocabTerm) (((PathValue) b).value())).key());

        return reduceByCondition(hasMoreSpecificTypeThan);
    }

    Node expandChildren(JsonLd jsonLd, Collection<String> rulingTypes) {
        return mapFilterAndReinstantiate(c -> c.expand(jsonLd, rulingTypes), Objects::nonNull);
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

    Node reduceByCondition(BiFunction<Node, Node, Boolean> condition) {
        List<Node> reduced = new ArrayList<>();
        children().stream()
                .map(child -> child instanceof Group g ? g.reduceByCondition(condition) : child)
                .sorted(Comparator.comparing(Group.class::isInstance))
                .forEach(child -> {
                    if (reduced.stream().noneMatch(otherChild -> implies(otherChild, child, condition))) {
                        reduced.add(child);
                    }
                });
        return reduced.size() == 1 ? reduced.getFirst() : newInstance(reduced);
    }

    // TODO: Review/refine nested logic and proper tests
    private Map<String, Object> toEsNested(Map<String, List<PathValue>> nestedGroups, EsMappings esMappings, EsBoost.Config boostConfig) {
        List<Map<String, Object>> esChildren = new ArrayList<>();
        List<Node> nonNested = new ArrayList<>(children());

        nestedGroups.forEach((nestedStem, group) -> {
            var bool = new HashMap<String, Object>();
            group.stream().collect(Collectors.groupingBy(pv -> pv.operator().equals(Operator.NOT_EQUALS)))
                    .forEach((k, v) -> {
                        boolean isGroup = v.size() > 1;
                        boolean isNegatedGroup = k && isGroup;
                        if (isNegatedGroup) {
                            var es = v.stream().map(pv -> pv.withOperator(Operator.EQUALS).getCoreEsQuery(esMappings, boostConfig)).toList();
                            bool.put("must_not", nestedWrap(nestedStem, wrap(es)));
                        } else {
                            var es = v.stream().map(pv -> pv.getCoreEsQuery(esMappings, boostConfig)).toList();
                            bool.put("must", nestedWrap(nestedStem, isGroup ? wrap(es) : es.getFirst()));
                        }
                    });
            esChildren.add(boolWrap(bool));
            nonNested.removeAll(group);
        });

        for (Node n : nonNested) {
            esChildren.add(n.toEs(esMappings, boostConfig));
        }

        return esChildren.size() == 1 ? esChildren.getFirst() : wrap(esChildren);
    }

    // TODO: Review/refine nested logic and proper tests
    private Map<String, List<PathValue>> getNestedGroups(EsMappings esMappings) {
        Map<String, List<PathValue>> nestedGroups = new HashMap<>();
        children().stream()
                .filter(PathValue.class::isInstance)
                .map(PathValue.class::cast)
                .collect(Collectors.groupingBy(pv -> pv.path().getEsNestedStem(esMappings),
                        Collectors.groupingBy(pv -> pv.path().jsonForm()))
                )
                .forEach((nestedStem, groupedByPath) -> {
                    // At least two different paths sharing the same nested stem
                    if (nestedStem.isPresent() && groupedByPath.size() > 1) {
                        // Don't group if the same path is repeated
                        if (groupedByPath.values().stream().noneMatch(l -> l.size() > 1)) {
                            List<PathValue> group = groupedByPath.values()
                                    .stream()
                                    .flatMap(List::stream)
                                    .toList();
                            nestedGroups.put(nestedStem.get(), group);
                        }
                    }
                });
        return nestedGroups;
    }

    private List<Map<String, Object>> childrenToEs(EsMappings esMappings, EsBoost.Config boostConfig) {
        return mapToMap(n -> n.toEs(esMappings, boostConfig));
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

package whelk.search2.querytree;

import whelk.search2.Disambiguate;
import whelk.search2.Operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.boolWrap;
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

    // Abstract class does not allow records as subclasses, however when comparing nodes we want the same behaviour
    // as for records, hence the following.
    @Override
    public boolean equals(Object o) {
        return o.getClass() == this.getClass() && ((Group) o).children().equals(children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }

    @Override
    public Map<String, Object> toEs() {
        List<List<PathValue>> nestedGroups = getNestedGroups();
        return nestedGroups.isEmpty() ? wrap(childrenToEs()) : toEsNested(nestedGroups);
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), mapToMap(c -> c.toSearchMapping(qt, nonQueryParams)));
        m.put("up", qt.makeUpLink(this, nonQueryParams));
        return m;
    }

    @Override
    public Node expand(Disambiguate disambiguate, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        Node reduced = reduceTypes(disambiguate);
        return switch (reduced) {
            case Group g -> {
                rulingTypes = Stream.concat(g.collectRulingTypes().stream(), rulingTypes.stream())
                        .collect(Collectors.toSet());
                yield g.expandChildren(disambiguate, rulingTypes, getBoostFields);
            }
            default -> reduced.expand(disambiguate, rulingTypes, getBoostFields);
        };
    }

    @Override
    public Group insertValue(Value value) {
        return mapAndReinstantiate(c -> c.insertValue(value));
    }

    @Override
    public Group insertOperator(Operator operator) {
        return mapAndReinstantiate(c -> c.insertOperator(operator));
    }

    @Override
    public Node insertNested(Function<String, Optional<String>> getNestedPath) {
        return mapAndReinstantiate(c -> c.insertNested(getNestedPath));
    }

    @Override
    public Node modifyAllPathValue(Function<PathValue, PathValue> modifier) {
        return mapAndReinstantiate(c -> c.modifyAllPathValue(modifier));
    }

    @Override
    public String toString(boolean topLevel) {
        String group = doMapToString(n -> n.toString(false))
                .collect(Collectors.joining(delimiter()));

        return topLevel ? group : "(" + group + ")";
    }

    @Override
    public Node reduceTypes(Disambiguate disambiguate) {
        BiFunction<Node, Node, Boolean> hasMoreSpecificTypeThan = (a, b) -> a.isTypeNode()
                && b.isTypeNode()
                && disambiguate.isSubclassOf(((PropertyValue) a).value().string(), ((PropertyValue) b).value().string());
        return reduceByCondition(hasMoreSpecificTypeThan);
    }

    Node expandChildren(Disambiguate disambiguate, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return mapFilterAndReinstantiate(c -> c.expand(disambiguate, rulingTypes, getBoostFields), Objects::nonNull);
    }

    List<Node> flattenChildren(List<Node> children) {
        return children.stream()
                .flatMap(c -> switch (c) {
                    case Group g -> g.getClass() == this.getClass()
                            ? g.children().stream()
                            : Stream.of(g);
                    default -> Stream.of(c);
                })
                .distinct()
                .toList();
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
    private Map<String, Object> toEsNested(List<List<PathValue>> nestedGroups) {
        List<Map<String, Object>> esChildren = new ArrayList<>();
        List<Node> nonNested = new ArrayList<>(children());

        for (List<PathValue> nodeList : nestedGroups) {
            var groupedByStem = nodeList.stream().collect(Collectors.groupingBy(PathValue::getNestedStem));
            if (groupedByStem.size() != 1) {
                throw new RuntimeException("Nested group must not contain different stems");
            }

            var stem = groupedByStem.keySet().iterator().next().get();
            var bool = new HashMap<String, Object>();

            nodeList.stream().collect(Collectors.groupingBy(pv -> pv.operator().equals(Operator.NOT_EQUALS)))
                    .forEach((k, v) -> {
                        var es = v.stream().map(PathValue::getEs).toList();
                        bool.put(k ? "must_not" : "must", nestedWrap(stem, es.size() > 1 ? wrap(es) : es.getFirst()));
                    });

            esChildren.add(boolWrap(bool));
            nonNested.removeAll(nodeList);
        }

        for (Node n : nonNested) {
            esChildren.add(n.toEs());
        }

        return esChildren.size() == 1 ? esChildren.getFirst() : wrap(esChildren);
    }

    // TODO: Review/refine nested logic and proper tests
    private List<List<PathValue>> getNestedGroups() {
        return children().stream()
                .filter(Group::childIsNested)
                .map(PathValue.class::cast)
                .collect(Collectors.groupingBy(PathValue::getNestedStem, Collectors.groupingBy(PathValue::getPath)))
                .values()
                .stream()
                .filter(groupedByPath -> groupedByPath.keySet().size() > 1 // At least two different paths with the same stem
                        && groupedByPath.values()
                        .stream()
                        .map(List::size)
                        .allMatch(samePathCount -> samePathCount < 2)) // Don't group if the same path is repeated
                .map(groupedByPath -> groupedByPath.values()
                        .stream()
                        .flatMap(List::stream)
                        .toList())
                .toList();
    }

    private static boolean childIsNested(Node n) {
        return n instanceof PathValue && ((PathValue) n).isNested();
    }

    private List<Map<String, Object>> childrenToEs() {
        return mapToMap(Node::toEs);
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

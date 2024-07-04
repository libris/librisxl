package whelk.search2.querytree;

import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.OutsetType;
import whelk.search2.QueryUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.nestedWrap;

public sealed interface Group extends Node permits And, Or {
    @Override
    List<Node> children();

    Group create(List<Node> children);

    String delimiter();

    String key();

    Map<String, Object> wrap(List<Map<String, Object>> esChildren);

    @Override
    default Map<String, Object> toEs(List<String> boostedFields) {
        List<List<PathValue>> nestedGroups = getNestedGroups();
        return nestedGroups.isEmpty() ? wrap(childrenToEs(boostedFields)) : toEsNested(nestedGroups, boostedFields);
    }

    @Override
    default Map<String, Object> toSearchMapping(QueryTree qt, Function<Value, Object> lookUp, Map<String, String> nonQueryParams) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), mapToMap(c -> c.toSearchMapping(qt, lookUp, nonQueryParams)));
        m.put("up", qt.makeUpLink(this, nonQueryParams));
        return m;
    }

    @Override
    default Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        return expandChildren(disambiguate, outsetType);
    }

    @Override
    default Group insertValue(Value value) {
        return mapAndRebuild(c -> c.insertValue(value));
    }

    @Override
    default Group insertOperator(Operator operator) {
        return mapAndRebuild(c -> c.insertOperator(operator));
    }

    @Override
    default Node insertNested(Function<String, Optional<String>> getNestedPath) {
        return mapAndRebuild(c -> c.insertNested(getNestedPath));
    }

    @Override
    default String toString(boolean topLevel) {
        String group = doMapToString(n -> n.toString(false))
                .collect(Collectors.joining(delimiter()));

        return topLevel ? group : "(" + group + ")";
    }

    default Group mapAndRebuild(Function<Node, Node> mapper) {
        return create(mapToNode(mapper));
    }

    default Node mapAndRebuild(Function<Node, Node> mapper, Predicate<Node> filter) {
        List<Node> children = mapToNode(mapper, filter);
        return switch (children.size()) {
            case 0 -> null;
            case 1 -> children.getFirst();
            default -> create(children);
        };
    }

    default List<Node> filter(Predicate<Node> p) {
        return children().stream().filter(p).collect(Collectors.toList());
    }

    private Map<String, Object> toEsNested(List<List<PathValue>> nestedGroups, List<String> boostedFields) {
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
            esChildren.add(n.toEs(boostedFields));
        }

        return esChildren.size() == 1 ? esChildren.getFirst() : wrap(esChildren);
    }

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

    private Node expandChildren(Disambiguate disambiguate, OutsetType outsetType) {
        return mapAndRebuild(c -> c.expand(disambiguate, outsetType), Objects::nonNull);
    }

    private List<Map<String, Object>> childrenToEs(List<String> boostedFields) {
        return mapToMap(c -> c.toEs(boostedFields), Objects::nonNull);
    }

    private List<Node> mapToNode(Function<Node, Node> mapper) {
        return mapToNode(mapper, x -> true);
    }

    private List<Node> mapToNode(Function<Node, Node> mapper, Predicate<Node> filter) {
        return children().stream().map(mapper).filter(filter).collect(Collectors.toList());
    }

    private List<Map<String, Object>> mapToMap(Function<Node, Map<String, Object>> mapper) {
        return mapToMap(mapper, x -> true);
    }

    private List<Map<String, Object>> mapToMap(Function<Node, Map<String, Object>> mapper,
                                         Predicate<Map<String, Object>> filter) {
        return children().stream().map(mapper).filter(filter).collect(Collectors.toList());
    }

    private Stream<String> doMapToString(Function<Node, String> mapper) {
        return children().stream().map(mapper);
    }
}

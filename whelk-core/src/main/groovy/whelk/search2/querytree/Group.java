package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.QueryUtil;

import java.util.*;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public sealed abstract class Group implements Node permits And, Or {
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
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        Map<Node, Node> nodeMap = new HashMap<>();
        List<Node> newChildren = new ArrayList<>();
        for (Node child : children()) {
            ExpandedNode expandedChild = child.expand(jsonLd, rdfSubjectTypes);
            if (!expandedChild.isEmpty()) {
                newChildren.add(expandedChild.expandedRoot());
                nodeMap.putAll(expandedChild.nodeMap());
            }
        }
        Node expandedRoot = switch (newChildren.size()) {
            case 0 -> null;
            case 1 -> newChildren.getFirst();
            default -> newInstance(newChildren);
        };
        nodeMap.put(this, expandedRoot);
        return new ExpandedNode(expandedRoot, nodeMap);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key(), children().stream().map(c -> c.toSearchMapping(makeUpLink)).toList());
        m.put("up", makeUpLink.apply(this));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = children().stream().map(n -> n.toQueryString(false))
                .collect(Collectors.joining(delimiter()));
        return topLevel ? s : QueryUtil.parenthesize(s);
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    public Node filterAndReinstantiate(Predicate<Node> p) {
        return mapFilterAndReinstantiate(Function.identity(), p);
    }

    public Group mapAndReinstantiate(Function<Node, Node> mapper) {
        return newInstance(children().stream().map(mapper).toList());
    }

    public Node mapFilterAndReinstantiate(Function<Node, Node> mapper, Predicate<Node> p) {
        List<Node> newChildren = children().stream().map(mapper).filter(p).toList();
        return switch (newChildren.size()) {
            case 0 -> null;
            case 1 -> newChildren.getFirst();
            default -> newInstance(newChildren);
        };
    }

    List<Node> flattenChildren(List<? extends Node> children) {
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

    List<Map<String, Object>> childrenToEs(ESSettings esSettings) {
        return children().stream().map(n -> n.toEs(esSettings)).toList();
    }

    Map<String, Object> getCoreEsQuery(ESSettings esSettings) {
        return wrap(children().stream().map(n -> toCoreEsQuery(n, esSettings)).toList());
    }

    private static Map<String, Object> toCoreEsQuery(Node node, ESSettings esSettings) {
        return switch (node) {
            case Condition c -> c.getCoreEsQuery(esSettings);
            case Group g -> g.getCoreEsQuery(esSettings);
            default -> node.toEs(esSettings);
        };
    }
}

package whelk.search2.querytree;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public sealed interface Node permits Any, Condition, FilterAlias, FreeText, Group, Not {
    Node getInverse();

    default List<Node> children() {
        return Collections.emptyList();
    }

    default Node deepMap(Function<Node, Node> mapper) {
        Node rebuilt = withMappedChildren(this, child -> child.deepMap(mapper), false);
        return mapper.apply(rebuilt);
    }

    default String toQueryString() {
        return QueryStringBuilder.buildFrom(this, true);
    }

    static Stream<Node> allDescendants(Node node) {
        return StreamSupport.stream(_allDescendants(node).spliterator(), false);
    }

    static Node withMappedChildren(Node node, UnaryOperator<Node> mapper) {
        return withMappedChildren(node, mapper, true);
    }

    static Node remove(Node tree, Collection<? extends Node> remove) {
        return remove.stream().anyMatch(n -> n == tree)
                ? null
                : withMappedChildren(tree, child -> remove(child, remove));
    }

    static Node replace(Node tree, Node replace, Node replacement) {
        return tree == replace
                ? replacement
                : withMappedChildren(tree, child -> replace(child, replace, replacement));
    }

    static Node add(Node tree, Node add) {
        return switch (tree) {
            case Any ignored -> add;
            case And and -> new And(Stream.concat(and.children().stream(), Stream.of(add)).distinct().toList());
            default -> tree.equals(add) ? tree : new And(List.of(tree, add));
        };
    }

    private static Node withMappedChildren(Node node, UnaryOperator<Node> mapper, boolean flatten) {
        return switch (node) {
            case Not n -> {
                var innerMapped = mapper.apply(n.node());
                yield innerMapped != null ? new Not(innerMapped) : null;
            }
            case Group g -> {
                List<Node> mappedChildren = g.children().stream()
                        .map(mapper)
                        .filter(Objects::nonNull)
                        .toList();
                yield switch (mappedChildren.size()) {
                    case 0 -> null;
                    case 1 -> mappedChildren.getFirst();
                    default -> g.newInstance(mappedChildren, flatten);
                };
            }
            default -> node;
        };
    }

    private static Iterable<Node> _allDescendants(Node node) {
        Iterator<Node> i = new Iterator<>() {
            List<Node> nodes;

            @Override
            public boolean hasNext() {
                if (nodes == null) {
                    nodes = new LinkedList<>();
                    nodes.add(node);
                }
                return !nodes.isEmpty();
            }

            @Override
            public Node next() {
                Node next = nodes.removeFirst();
                nodes.addAll(next.children());
                return next;
            }
        };

        return () -> node != null ? i : Collections.emptyIterator();
    }
}
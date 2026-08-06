package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public sealed interface Node permits Any, Condition, FilterAlias, FreeText, Group, Not {
    Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink, BiFunction<Node, Node, Map<String, String>> makeReplaceLink);

    String toQueryString(boolean topLevel);

    Node getInverse();

    Node reduce(JsonLd jsonLd);

    boolean implies(Node node, JsonLd jsonLd);

    RdfSubjectType rdfSubjectType();

    default boolean implies(Node node, Predicate<Node> cmp) {
        return switch (node) {
            case And and -> and.children().stream().allMatch(cmp);
            case Or or -> or.children().stream().anyMatch(cmp);
            default -> cmp.test(node);
        };
    }

    default Stream<Node> allDescendants() {
        return QueryTree.allDescendants(this);
    }

    default List<Node> children() {
        return Collections.emptyList();
    }

    default Node deepMap(Function<Node, Node> mapper) {
        Node rebuilt = withMappedChildren(this, child -> child.deepMap(mapper), false);
        return mapper.apply(rebuilt);
    }

    static Node withMappedChildren(Node node, UnaryOperator<Node> mapper) {
        return withMappedChildren(node, mapper, true);
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
}
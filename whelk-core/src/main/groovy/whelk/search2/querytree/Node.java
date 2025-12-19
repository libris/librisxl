package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public sealed interface Node permits FilterAlias, FreeText, Group, Not, Condition {
    Map<String, Object> toEs(ESSettings esSettings);

    ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes);

    Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink);

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
}
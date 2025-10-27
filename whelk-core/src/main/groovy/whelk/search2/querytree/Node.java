package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.QueryParams;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public sealed interface Node permits FilterAlias, FreeText, Group, Not, PathValue {
    Map<String, Object> toEs(ESSettings esSettings);

    Node expand(JsonLd jsonLd, Collection<String> subjectTypes);

    Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink);

    String toQueryString(boolean topLevel);

    Node getInverse();

    Node reduce(JsonLd jsonLd);

    boolean implies(Node node, JsonLd jsonLd);

    default boolean implies(Node node, Predicate<Node> cmp) {
        return switch (node) {
            case And and -> and.children().stream().allMatch(cmp);
            case Or or -> or.children().stream().anyMatch(cmp);
            default -> cmp.test(node);
        };
    }

    default List<Node> children() {
        return Collections.emptyList();
    }

    default List<String> subjectTypesList() {
        return List.of();
    }

    default Optional<Node> subjectTypesNode() {
        return Optional.empty();
    }
}
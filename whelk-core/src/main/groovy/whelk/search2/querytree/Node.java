package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public sealed interface Node permits ActiveBoolFilter, FreeText, Group, InactiveBoolFilter, PathValue {
    Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath, Collection<String> boostFields);

    Node expand(JsonLd jsonLd, Collection<String> rulingTypes);

    Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams);

    String toQueryString(boolean topLevel);

    default List<Node> children() {
        return Collections.emptyList();
    }

    default Node reduceTypes(JsonLd jsonLd) {
        return this;
    }

    default boolean isTypeNode() {
        return false;
    }
}
package whelk.search2.querytree;

import whelk.search2.Disambiguate;
import whelk.search2.Operator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public sealed interface Node permits ActiveBoolFilter, FreeText, Group, InactiveBoolFilter, PathValue, PropertyValue {
    Map<String, Object> toEs();

    Node expand(Disambiguate disambiguate, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields);

    Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams);

    String toString(boolean topLevel);

    default Node insertOperator(Operator operator) {
        return this;
    }

    default Node insertValue(Value value) {
        return this;
    }

    default Node insertNested(Function<String, Optional<String>> getNestedPath) {
        return this;
    }

    default Node modifyAllPathValue(Function<PathValue, PathValue> modifier) {
        return this;
    }

    default List<Node> children() {
        return Collections.emptyList();
    }

    default Node reduceTypes(Disambiguate disambiguate) {
        return this;
    }

    default boolean isTypeNode() {
        return false;
    }

    default List<String> collectTypes(Disambiguate disambiguate) {
        return List.of();
    }
}
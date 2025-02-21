package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public record InactiveBoolFilter(String alias) implements Node {
    @Override
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return null;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return "NOT " + alias;
    }
}
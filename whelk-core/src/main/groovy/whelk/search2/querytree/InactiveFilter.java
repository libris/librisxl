package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Filter;
import whelk.search2.QueryParams;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public record InactiveFilter(Filter.AliasedFilter aliasedFilter) implements Node {
    @Override
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath, Collection<String> boostFields) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return null;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return toString();
    }

    @Override
    public String toString() {
        return "NOT " + alias();
    }

    @Override
    public Node getInverse() {
        return filter();
    }

    public Node filter() {
        return new ActiveFilter(aliasedFilter);
    }

    private String alias() {
        return aliasedFilter.alias();
    }
}
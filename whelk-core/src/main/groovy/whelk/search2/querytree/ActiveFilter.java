package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Filter;
import whelk.search2.QueryParams;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static whelk.search2.QueryUtil.makeUpLink;

public record ActiveFilter(Filter.AliasedFilter aliasedFilter) implements Node {
    @Override
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath, Collection<String> boostFields) {
        throw new UnsupportedOperationException("Expand filter before converting to ES");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        var m = new LinkedHashMap<String, Object>();
        m.put("object", description());
        m.put("value", alias());
        m.put("up", makeUpLink(qt, this, queryParams));
        return m;
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return aliasedFilter.getParsed().expand(jsonLd, rulingTypes);
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return toString();
    }

    @Override
    public String toString() {
        return alias();
    }

    @Override
    public Node getInverse() {
        return filter() instanceof InactiveFilter(Filter.AliasedFilter af)
                ? new ActiveFilter(af)
                : new InactiveFilter(aliasedFilter);
    }

    public Node filter() {
        return aliasedFilter.getParsed();
    }

    public String alias() {
        return aliasedFilter.alias();
    }

    public Map<String, Object> description() {
        return aliasedFilter.description();
    }
}

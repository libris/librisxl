package whelk.search2.querytree;

import whelk.search2.Disambiguate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record InactiveBoolFilter(String alias) implements Node {
    @Override
    public Map<String, Object> toEs() {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Node expand(Disambiguate disambiguate, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return null;
    }

    @Override
    public String toString(boolean topLevel) {
        return "NOT " + alias;
    }
}
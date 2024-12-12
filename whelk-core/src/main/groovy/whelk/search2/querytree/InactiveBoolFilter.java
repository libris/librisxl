package whelk.search2.querytree;

import whelk.search2.Disambiguate;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public record InactiveBoolFilter(String alias) implements Node {
    @Override
    public Map<String, Object> toEs(List<String> boostedFields) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        throw new UnsupportedOperationException("Query tree must not contain inactive filters");
    }

    @Override
    public Node expand(Disambiguate disambiguate, String queryBaseType) {
        return null;
    }

    @Override
    public String toString(boolean topLevel) {
        return "NOT " + alias;
    }
}
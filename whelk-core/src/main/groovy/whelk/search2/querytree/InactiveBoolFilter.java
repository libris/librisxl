package whelk.search2.querytree;

import whelk.search2.Disambiguate;
import whelk.search2.OutsetType;
import whelk.search2.QueryUtil;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    public Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        return null;
    }

    @Override
    public String toString(boolean topLevel) {
        return "NOT " + alias;
    }
}
package whelk.search2.querytree;

import whelk.search2.Disambiguate;
import whelk.search2.OutsetType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record ActiveBoolFilter(String alias, Node filter, Map<?, ?> prefLabelByLang) implements Node {
    @Override
    public Map<String, Object> toEs(List<String> boostedFields) {
        throw new UnsupportedOperationException("Expand filter before converting to ES");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        var m = new LinkedHashMap<String, Object>();
        m.put("object", Map.of("prefLabelByLang", prefLabelByLang));
        m.put("value", alias);
        m.put("up", qt.makeUpLink(this, nonQueryParams));
        return m;
    }

    @Override
    public Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        return filter.expand(disambiguate, outsetType);
    }

    @Override
    public String toString(boolean topLevel) {
        return alias;
    }

    public boolean nullifies(ActiveBoolFilter abf) {
        return filter instanceof InactiveBoolFilter
                && ((InactiveBoolFilter) filter).alias().equals(abf.alias());
    }
}

package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.parenthesize;

public record Not(Node node) implements Node {
    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return QueryUtil.mustNotWrap(node.toEs(esSettings));
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return node instanceof Filter.AliasedFilter
                ? null
                : new Not(node.expand(jsonLd, rulingTypes));
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        return Map.of("not", node.toSearchMapping(qt, queryParams),
                "up", makeUpLink(qt, this, queryParams));
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = node instanceof FreeText ft && ft.isMultiToken()
                ? parenthesize(ft.toQueryString(true))
                :  node.toQueryString(false);
        return "NOT " + s;
    }

    @Override
    public String toString() {
        return "NOT " + node.toQueryString(false);
    }

    @Override
    public Node getInverse() {
        return node;
    }

    @Override
    public List<Node> children() {
        return List.of(node);
    }
}

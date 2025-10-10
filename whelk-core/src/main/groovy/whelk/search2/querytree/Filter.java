package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.ESSettings;
import whelk.search2.QueryParams;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.JsonLd.Rdfs.RESOURCE;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.search2.QueryUtil.makeUpLink;

public sealed class Filter {
    private final String raw;
    private Node parsed;

    public Filter(String raw) {
        this.raw = raw;
    }

    public Node getParsed() {
        if (parsed == null) {
            throw new IllegalStateException("Filter has not been parsed");
        }
        return parsed;
    }

    public String getRaw() {
        return raw;
    }

    public void parse(Disambiguate disambiguate) throws InvalidQueryException {
        if (parsed == null) {
            this.parsed = QueryTreeBuilder.buildTree(raw, disambiguate);
        }
    }

    public boolean isTypeFilter() {
        return getParsed().isTypeNode() || (getParsed() instanceof Group g && g.children().stream().allMatch(Node::isTypeNode));
    }

    public static sealed class AliasedFilter extends Filter implements Node {
        private final String alias;
        private final Map<String, Object> prefLabelByLang;

        public AliasedFilter(String alias, String raw, Map<String, Object> prefLabelByLang) {
            super(raw);
            this.alias = alias;
            this.prefLabelByLang = prefLabelByLang;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            throw new UnsupportedOperationException("Expand filter before converting to ES");
        }

        @Override
        public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
            var m = new LinkedHashMap<String, Object>();
            LinkedHashMap<String, Object> description = new LinkedHashMap<>(description());
            description.put("parsedFilter", getParsed().toSearchMapping(qt, queryParams));
            m.put("object", description);
            m.put("value", alias());
            m.put("up", makeUpLink(qt, this, queryParams));
            return m;
        }

        @Override
        public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
            return getParsed().expand(jsonLd, rulingTypes);
        }

        @Override
        public String toQueryString(boolean topLevel) {
            return alias();
        }

        @Override
        public String toString() {
            return alias();
        }

        @Override
        public Node getInverse() {
            return getParsed() instanceof Not(Filter.AliasedFilter af) ? af : new Not(this);
        }

        public Map<String, Object> description() {
            return Map.of(TYPE_KEY, RESOURCE,
                    "prefLabelByLang", prefLabelByLang,
                    "alias", alias,
                    "raw", this.getRaw()
            );
        }

        public String alias() {
            return alias;
        }
    }

    public static final class QueryDefinedFilter extends AliasedFilter {
        public QueryDefinedFilter(String alias, String raw, Map<String, Object> prefLabelByLang) {
            super(alias, raw, prefLabelByLang);
        }
    }
}

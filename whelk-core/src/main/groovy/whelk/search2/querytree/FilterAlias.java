package whelk.search2.querytree;

import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;

import java.util.Map;

import static whelk.JsonLd.Rdfs.RESOURCE;
import static whelk.JsonLd.TYPE_KEY;

public sealed class FilterAlias implements Node {
    private final String alias;
    private final String raw;
    private final Map<String, Object> prefLabelByLang;

    private Node parsed;

    public FilterAlias(String alias, String raw, Map<String, Object> prefLabelByLang) {
        this.raw = raw;
        this.alias = alias;
        this.prefLabelByLang = prefLabelByLang;
    }

    @Override
    public Node getInverse() {
        return getParsed() instanceof Not(FilterAlias fa) ? fa : new Not(this);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return parsed.rdfSubjectType();
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FilterAlias otherFa && alias().equals(otherFa.alias());
    }

    @Override
    public int hashCode() {
        return alias().hashCode();
    }

    public void parse(Disambiguate disambiguate) throws InvalidQueryException {
        if (parsed == null) {
            this.parsed = QueryTreeBuilder.buildTree(raw, disambiguate);
        }
    }

    public Node getParsed() {
        if (parsed == null) {
            throw new IllegalStateException("Filter has not been parsed");
        }
        return parsed;
    }

    public String alias() {
        return alias;
    }

    public Map<String, Object> description() {
        return Map.of(TYPE_KEY, RESOURCE,
                "prefLabelByLang", prefLabelByLang,
                "alias", alias,
                "raw", raw
        );
    }

    public static final class QueryDefinedAlias extends FilterAlias {
        public QueryDefinedAlias(String alias, String raw) {
            super(alias, raw, Map.of());
        }
    }
}

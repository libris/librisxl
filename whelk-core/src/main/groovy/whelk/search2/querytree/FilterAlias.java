package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.ESSettings;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

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
    public Map<String, Object> toEs(ESSettings esSettings) {
        return getParsed().toEs(esSettings);
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        ExpandedNode expanded = getParsed().expand(jsonLd, rdfSubjectTypes);
        Map<Node, Node> nodeMap = new HashMap<>();
        nodeMap.put(this, expanded.expandedRoot());
        nodeMap.putAll(expanded.nodeMap());
        return new ExpandedNode(expanded.expandedRoot(), nodeMap);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        var m = new LinkedHashMap<String, Object>();
        LinkedHashMap<String, Object> description = new LinkedHashMap<>(description());
        description.put("parsedFilter", getParsed().toSearchMapping(makeUpLink));
        m.put("object", description);
        m.put("value", alias());
        m.put("up", makeUpLink.apply(this));
        return m;
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
        return getParsed() instanceof Not(FilterAlias fa) ? fa : new Not(this);
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return this;
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return implies(node, this::equals) || getParsed().implies(node, jsonLd);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
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

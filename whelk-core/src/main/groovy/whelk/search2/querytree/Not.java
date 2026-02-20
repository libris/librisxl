package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.QueryUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static whelk.search2.QueryUtil.parenthesize;

public record Not(Node node) implements Node {
    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return QueryUtil.mustNotWrap(node.toEs(esSettings));
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (node instanceof FilterAlias) {
            return ExpandedNode.newEmpty();
        }
        ExpandedNode expandedChild = node.expand(jsonLd, rdfSubjectTypes);
        Node expandedRoot = new Not(expandedChild.expandedRoot());
        Map<Node, Node> nodeMap = new HashMap<>(expandedChild.nodeMap());
        nodeMap.put(this, expandedRoot);
        return new ExpandedNode(expandedRoot, nodeMap);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        return Map.of("not", node.toSearchMapping(makeUpLink),
                "up", makeUpLink.apply(this));
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
    public Node reduce(JsonLd jsonLd) {
        return this;
    }

    @Override
    public boolean implies(Node other, JsonLd jsonLd) {
        if (node instanceof FilterAlias || other instanceof Not(FilterAlias fa)) {
            return equals(other);
        }
        Node inverse = other.getInverse();
        return !(inverse instanceof Not) && inverse.implies(node, jsonLd);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public List<Node> children() {
        return List.of(node);
    }
}

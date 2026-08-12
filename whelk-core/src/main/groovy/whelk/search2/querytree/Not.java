package whelk.search2.querytree;

import java.util.List;

import static whelk.search2.QueryUtil.parenthesize;

public record Not(Node node) implements Node {
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
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public List<Node> children() {
        return List.of(node);
    }
}

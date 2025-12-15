package whelk.search2.querytree;

import whelk.JsonLd;

public class ReducedQueryTree extends QueryTree {
    public ReducedQueryTree(Node tree) {
        super(tree);
    }

    public static ReducedQueryTree newEmpty() {
        return new ReducedQueryTree(null);
    }

    @Override
    public ReducedQueryTree reduce(JsonLd jsonLd) {
        return this;
    }
}

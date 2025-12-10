package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Map;

public class ExpandedQueryTree extends QueryTree {
    private final Map<Node, Node> nodeMap;

    public ExpandedQueryTree(ExpandedNode expandedNode) {
        this(expandedNode.expandedRoot(), expandedNode.nodeMap());
    }

    public Map<Node, Node> nodeMap() {
        return nodeMap;
    }

    public static ExpandedQueryTree newEmpty() {
        return new ExpandedQueryTree(ExpandedNode.newEmpty());
    }

    @Override
    public ExpandedQueryTree expand(JsonLd jsonLd) {
        return this;
    }

    @Override
    public ExpandedQueryTree copy() {
        return new ExpandedQueryTree(tree(), nodeMap);
    }

    private ExpandedQueryTree(Node tree, Map<Node, Node> nodeMap) {
        super(tree);
        this.nodeMap = nodeMap;
    }
}

package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;

import java.util.Map;

public class ExpandedQueryTree extends QueryTree {
    private final Map<Node, Node> nodeMap;

    public ExpandedQueryTree(ExpandedNode expandedNode) {
        this(expandedNode.expandedRoot(), expandedNode.nodeMap());
    }

    public Map<String, Object> toEs(ESSettings esSettings) {
        return new EsQueryTree(this, esSettings).getMainQuery();
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

    private ExpandedQueryTree(Node tree, Map<Node, Node> nodeMap) {
        super(tree);
        this.nodeMap = nodeMap;
    }
}

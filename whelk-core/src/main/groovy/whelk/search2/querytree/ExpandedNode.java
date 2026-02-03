package whelk.search2.querytree;

import java.util.Map;

public record ExpandedNode(
        Node expandedRoot,
        Map<Node, Node> nodeMap   // original -> expanded for every sub-node in the original tree
) {
    public boolean isEmpty() {
        return expandedRoot instanceof Any;
    }

    public static ExpandedNode identity(Node n) {
        return new ExpandedNode(n, Map.of(n, n));
    }

    public static ExpandedNode newEmpty() {
        return new ExpandedNode(new Any.EmptyString(), Map.of());
    }

    @Override
    public String toString() {
        return expandedRoot.toString();
    }
}

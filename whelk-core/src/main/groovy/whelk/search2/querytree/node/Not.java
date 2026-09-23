package whelk.search2.querytree.node;

import java.util.List;

public record Not(Node node) implements Node {
    @Override
    public Node getInverse() {
        return node;
    }

    @Override
    public List<Node> children() {
        return List.of(node);
    }

    @Override
    public String toString() {
        return toQueryString();
    }
}

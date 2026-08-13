package whelk.search2.querytree.node;

import java.util.HashSet;
import java.util.List;

public non-sealed class Or extends Group {
    private final List<Node> children;

    public Or(List<? extends Node> children) {
       this(children, true);
    }

    public Or(List<? extends Node> children, boolean flattenChildren) {
        this.children = flattenChildren
                ? flattenChildren(children)
                : children.stream().map(Node.class::cast).toList();
    }

    @Override
    public Node getInverse() {
        return new And(children.stream().map(Node::getInverse).toList());
    }

    @Override
    public List<Node> children() {
        return children;
    }

    @Override
    public Or newInstance(List<Node> children, boolean flattenChildren) {
        return new Or(children, flattenChildren);
    }

    @Override
    public String delimiter() {
        return " OR ";
    }

    @Override
    public String key() {
        return "or";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Or other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }
}


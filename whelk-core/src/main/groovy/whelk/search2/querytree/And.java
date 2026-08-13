package whelk.search2.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;

public non-sealed class And extends Group {
    private final List<Node> children;

    public And(List<? extends Node> children) {
        this(children, true);
    }

    public And(List<? extends Node> children, boolean flattenChildren) {
        this.children = flattenChildren
                ? flattenChildren(children)
                : children.stream().map(Node.class::cast).toList();
    }

    @Override
    public Node getInverse() {
        return new Or(children.stream().map(Node::getInverse).toList());
    }

    @Override
    public List<Node> children() {
        return children;
    }

    @Override
    public And newInstance(List<Node> children, boolean flattenChildren) {
        return new And(children, flattenChildren);
    }

    @Override
    public String delimiter() {
        return " ";
    }

    @Override
    public String key() {
        return "and";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof And other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }
}

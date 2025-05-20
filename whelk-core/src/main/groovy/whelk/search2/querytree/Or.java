package whelk.search2.querytree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.shouldWrap;

public final class Or extends Group {
    private final List<Node> children;

    public Or(List<Node> children) {
        this(children, true);
    }

    // For test only
    public Or(List<Node> children, boolean flattenChildren) {
        this.children = flattenChildren ? flattenChildren(children) : children;
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
    public Group newInstance(List<Node> children) {
        return new Or(children);
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
    public Map<String, Object> wrap(List<Map<String, Object>> esChildren) {
        return shouldWrap(esChildren);
    }

    @Override
    List<String> collectRulingTypes() {
        return List.of();
    }

    @Override
    boolean implies(Node a, Node b, BiFunction<Node, Node, Boolean> condition) {
        if (a instanceof Group aGroup) {
            return b instanceof Group bGroup
                    ? bGroup.children().stream().anyMatch(child -> implies(a, child, condition))
                    : aGroup.children().stream().anyMatch(child -> condition.apply(child, b));
        } else {
            return b instanceof Group bGroup
                    ? bGroup.children().stream().anyMatch(child -> condition.apply(a, child))
                    : condition.apply(a, b);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Or other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }
}


package whelk.search2.querytree;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

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
}


package whelk.search2.querytree;

import whelk.search2.Operator;

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
        return switch (a) {
            case Group aGroup -> switch (b) {
                case Group bGroup -> bGroup.children().stream().anyMatch(child -> implies(a, child, condition));
                default -> aGroup.children().stream().anyMatch(child -> condition.apply(child, b));
            };
            default -> switch (b) {
                case Group g -> g.children().stream().anyMatch(child -> condition.apply(a, child));
                default -> condition.apply(a, b);
            };
        };
    }

    @Override
    public Group insertOperator(Operator operator) {
        return operator == Operator.NOT_EQUALS
                ? new And(children).insertOperator(operator)
                : super.insertOperator(operator);
    }
}


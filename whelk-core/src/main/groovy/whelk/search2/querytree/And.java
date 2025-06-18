package whelk.search2.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.mustWrap;

public final class And extends Group {
    private final List<Node> children;

    public And(List<Node> children) {
        this(children, true);
    }

    // For test only
    public And(List<Node> children, boolean flattenChildren) {
        this.children = flattenChildren ? flattenChildren(children) : children;
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
    public Group newInstance(List<Node> children) {
        return new And(children);
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
    public Map<String, Object> wrap(List<Map<String, Object>> esChildren) {
        return mustWrap(esChildren);
    }

    @Override
    List<String> collectRulingTypes() {
        return children().stream()
                .filter(n -> n.isTypeNode() || (n instanceof Or && n.children().stream().allMatch(Node::isTypeNode)))
                .flatMap(n -> n instanceof Or ? n.children().stream() : Stream.of(n))
                .map(PathValue.class::cast)
                .map(PathValue::value)
                .map(VocabTerm.class::cast)
                .map(VocabTerm::jsonForm)
                .toList();
    }

    @Override
    boolean implies(Node a, Node b, BiFunction<Node, Node, Boolean> condition) {
        if (a instanceof Group aGroup) {
            return b instanceof Group bGroup
                    ? bGroup.children().stream().allMatch(child -> implies(aGroup, child, condition))
                    : aGroup.children().stream().anyMatch(child -> condition.apply(child, b));
        } else {
            return switch (b) {
                case And and -> and.children().stream().allMatch(child -> condition.apply(a, child));
                case Or or -> or.children().stream().anyMatch(child -> condition.apply(a, child));
                default -> condition.apply(a, b);
            };
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof And other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }
}

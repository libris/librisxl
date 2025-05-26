package whelk.search2.querytree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
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
                .map(Value::jsonForm)
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

    public boolean contains(Node node) {
        return new HashSet<>(children).containsAll(node instanceof And ? node.children() : List.of(node));
    }

    public Node remove(Node node) {
        if (!contains(node)) {
            return this;
        }
        var filter = Predicate.not(node instanceof And ? ((And) node)::contains : node::equals);
        return filterAndReinstantiate(filter);
    }

    public Node add(Node node) {
        List<Node> newChildren = new ArrayList<>(children);
        (node instanceof And ? node.children().stream() : Stream.of(node))
                .filter(Predicate.not(children::contains))
                .forEach(newChildren::add);
        return new And(newChildren);
    }

    public Node replace(Node old, Node replacement) {
        if (!contains(old)) {
            return this;
        }

        return Optional.ofNullable(remove(old))
                .map(List::of)
                .map(And::new)
                .map(oldRemoved -> oldRemoved.add(replacement))
                .map(replaced -> replaced.children().size() == 1 ? replaced.children().getFirst() : replaced)
                .orElse(replacement);
    }

    public Optional<Node> findChild(Predicate<Node> condition) {
        return children.stream().filter(condition).findFirst();
    }
}

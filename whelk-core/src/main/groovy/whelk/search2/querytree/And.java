package whelk.search2.querytree;

import whelk.search2.Disambiguate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
                .map(PropertyValue.class::cast)
                .map(PropertyValue::value)
                .map(Value::string)
                .toList();
    }

    @Override
    boolean implies(Node a, Node b, BiFunction<Node, Node, Boolean> condition) {
        return switch (a) {
            case Group g -> switch (b) {
                case And and -> and.children().stream().allMatch(child -> implies(a, child, condition));
                case Or or -> or.children().stream().anyMatch(child -> implies(a, child, condition));
                default -> g.children().stream().anyMatch(child -> condition.apply(child, b));
            };
            default -> switch (b) {
                case And and -> and.children().stream().allMatch(child -> condition.apply(a, child));
                case Or or -> or.children().stream().anyMatch(child -> condition.apply(a, child));
                default -> condition.apply(a, b);
            };
        };
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

    public And add(Node node) {
        List<Node> newChildren = Stream.concat(children.stream(), node instanceof And ? node.children().stream() : Stream.of(node))
                .distinct()
                .toList();

        return new And(newChildren);
    }

    public Node replace(Node old, Node replacement) {
        var replaced = new And(List.of(remove(old))).add(replacement);
        return replaced.children().size() == 1 ? replaced.children().getFirst() : replaced;
    }
}

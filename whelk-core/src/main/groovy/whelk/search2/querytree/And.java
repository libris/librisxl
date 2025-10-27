package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
    public Node expand(JsonLd jsonLd, Collection<String> subjectTypes) {
        List<String> subjectTypesInGroup = subjectTypesList();
        return mapFilterAndReinstantiate(c -> c.expand(jsonLd, subjectTypesInGroup.isEmpty() ? subjectTypes : subjectTypesInGroup), Objects::nonNull);
    }

    @Override
    public Node getInverse() {
        return new Or(children.stream().map(Node::getInverse).toList());
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return node instanceof And
                ? node.children().stream().allMatch(child -> implies(child, jsonLd))
                : children.stream().anyMatch(child -> child.implies(node, jsonLd));
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
    public Optional<Node> subjectTypesNode() {
        return children().stream()
                .filter(n -> n instanceof Type || (n instanceof Or && n.children().stream().allMatch(Type.class::isInstance)))
                .findFirst();
    }

    @Override
    public List<String> subjectTypesList() {
        return subjectTypesNode().map(n -> n instanceof Type t
                        ? List.of(t.type())
                        : n.children().stream().map(Type.class::cast).map(Type::type).toList())
                .orElse(List.of());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof And other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return reduce(jsonLd, (a, b) -> pick(a, b, jsonLd));
    }

    private Optional<Node> pick(Node a, Node b, JsonLd jsonLd) {
        if (a.implies(b, jsonLd)) {
            return Optional.of(a);
        }
        if (b.implies(a, jsonLd)) {
            return Optional.of(b);
        }
        return Optional.empty();
    }
}

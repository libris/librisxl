package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
    public RdfSubjectType rdfSubjectType() {
        return children().stream()
                .map(Node::rdfSubjectType)
                .filter(Predicate.not(RdfSubjectType::isNoType))
                .findFirst()
                .orElse(RdfSubjectType.noType());
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

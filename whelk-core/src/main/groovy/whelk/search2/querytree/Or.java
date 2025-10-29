package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
    public Node expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return mapFilterAndReinstantiate(c -> c.expand(jsonLd, rdfSubjectTypes), Objects::nonNull);
    }

    @Override
    public Node getInverse() {
        return new And(children.stream().map(Node::getInverse).toList());
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return children.stream().allMatch(child -> child.implies(node, jsonLd));
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return children.stream().allMatch(Type.class::isInstance) ? new RdfSubjectType(this) : RdfSubjectType.noType();
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
    public boolean equals(Object o) {
        return o instanceof Or other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return reduce(jsonLd, (a, b) -> pick(a, b, jsonLd));
    }

    private Optional<Node> pick(Node a, Node b, JsonLd jsonLd) {
        if (a.implies(b, jsonLd)) {
            return Optional.of(b);
        }
        if (b.implies(a, jsonLd)) {
            return Optional.of(a);
        }
        return Optional.empty();
    }
}


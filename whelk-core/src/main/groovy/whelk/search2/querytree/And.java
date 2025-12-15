package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.nestedWrap;

public sealed class And extends Group {
    private final List<Node> children;

    public And(List<? extends Node> children) {
        this.children = flattenChildren(children);
    }

    public And(List<Node> children, boolean flattenChildren) {
        this.children = flattenChildren ? flattenChildren(children) : children;
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return mustWrap(childrenToEs(esSettings));
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        List<String> rdfSubjectTypesInGroup = rdfSubjectType().asList().stream().map(Type::type).toList();
        return super.expand(jsonLd, rdfSubjectTypesInGroup.isEmpty() ? rdfSubjectTypes : rdfSubjectTypesInGroup);
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
    Map<String, Object> wrap(List<Map<String, Object>> esChildren) {
        return mustWrap(esChildren);
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

    public static final class Nested extends And {
        private final String stem;

        public Nested(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, getCoreEsQuery(esSettings));
        }
    }
}

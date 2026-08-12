package whelk.search2.querytree;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

public non-sealed class Or extends Group {
    private final List<Node> children;

    public Or(List<? extends Node> children) {
       this(children, true);
    }

    public Or(List<? extends Node> children, boolean flattenChildren) {
        this.children = flattenChildren
                ? flattenChildren(children)
                : children.stream().map(Node.class::cast).toList();
    }

    @Override
    public Node getInverse() {
        return new And(children.stream().map(Node::getInverse).toList());
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return children.stream()
                .map(Node::rdfSubjectType)
                .noneMatch(RdfSubjectType::isNoType)
                    ? new RdfSubjectType(new Or(children.stream().flatMap(this::flattenDescendantTypeNodes).toList()))
                    : RdfSubjectType.noType();
    }

    private Stream<Node> flattenDescendantTypeNodes(Node n) {
        return switch(n) {
            case Type t -> Stream.of(t);
            case And and -> Stream.of(and.children().stream()
                    .filter((n2) -> n2 instanceof Type)
                    .findAny()
                    .orElseThrow(() -> new RuntimeException("couldn't find a Type for rdfSubjectType")));
            case Or or -> or.children().stream().flatMap(this::flattenDescendantTypeNodes).toList().stream();
            default -> throw new RuntimeException("couldn't map Node to Type");
        };
    }

    @Override
    public List<Node> children() {
        return children;
    }

    @Override
    public Or newInstance(List<Node> children, boolean flattenChildren) {
        return new Or(children, flattenChildren);
    }

    @Override
    String delimiter() {
        return " OR ";
    }

    @Override
    public String key() {
        return "or";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Or other && new HashSet<>(other.children()).equals(new HashSet<>(children));
    }
}


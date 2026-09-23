package whelk.search2.querytree;

import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;

import java.util.List;
import java.util.stream.Stream;

public class RdfSubjectType {
    private final Node typeNode;

    private List<Condition.Type> types;

    private RdfSubjectType(Node node) {
        this.typeNode = node;
    }

    public static RdfSubjectType extractFrom(Node node) {
        return switch (node) {
            case Condition.Type type -> new RdfSubjectType(type);
            case And and -> extractFromAnd(and);
            case Or or -> extractFromOr(or);
            default -> noType();
        };
    }

    public Node typeNode() {
        return typeNode;
    }

    public boolean hasType() {
        return typeNode != null;
    }

    public boolean isSingleType() {
        return types().size() == 1;
    }

    public boolean isMultiType() {
        return types().size() > 1;
    }

    public Condition.Type singleType() {
        assert isSingleType();
        return types().getFirst();
    }

    public List<Condition.Type> types() {
        if (types == null) {
            if (typeNode == null) {
                this.types = List.of();
            } else if (typeNode instanceof Condition.Type t) {
                this.types = List.of(t);
            } else {
                this.types = typeNode.children().stream()
                        .map(Condition.Type.class::cast)
                        .toList();
            }
        }
        return types;
    }

    public List<String> typeNames() {
        return types().stream()
                .map(Condition.Type::type)
                .toList();
    }

    public static RdfSubjectType noType() {
        return new RdfSubjectType(null);
    }

    private static RdfSubjectType extractFromAnd(And and) {
        return and.children().stream()
                .map(RdfSubjectType::extractFrom)
                .filter(RdfSubjectType::hasType)
                .findFirst()
                .orElse(noType());
    }

    private static RdfSubjectType extractFromOr(Or or) {
        return or.children().stream()
                .map(RdfSubjectType::extractFrom)
                .allMatch(RdfSubjectType::hasType)
                ? new RdfSubjectType(new Or(or.children().stream().flatMap(RdfSubjectType::extractDescendantTypeNodes).toList()))
                : RdfSubjectType.noType();
    }

    private static Stream<Node> extractDescendantTypeNodes(Node n) {
        return switch(n) {
            case Condition.Type t -> Stream.of(t);
            case And and -> and.children().stream()
                    .filter(Condition.Type.class::isInstance)
                    .findAny()
                    .stream();
            case Or or -> or.children().stream().flatMap(RdfSubjectType::extractDescendantTypeNodes);
            default -> throw new RuntimeException("couldn't map Node to Type");
        };
    }
}

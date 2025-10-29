package whelk.search2.querytree;

import java.util.List;

public class RdfSubjectType {
    private final Node typeNode;

    private List<Type> list;

    public RdfSubjectType(Node node) {
        this.typeNode = node;
    }

    public Node asNode() {
        return typeNode;
    }

    public boolean isNoType() {
        return typeNode == null;
    }

    public boolean isSingleType() {
        return asList().size() == 1;
    }

    public boolean isMultiType() {
        return asList().size() > 1;
    }

    public Type singleType() {
        assert isSingleType();
        return asList().getFirst();
    }

    public List<Type> asList() {
        if (list == null) {
            if (typeNode == null) {
                this.list = List.of();
            } else if (typeNode instanceof Type t) {
                this.list = List.of(t);
            } else {
                this.list = typeNode.children().stream().map(Type.class::cast).toList();
            }
        }
        return list;
    }

    public static RdfSubjectType noType() {
        return new RdfSubjectType(null);
    }
}

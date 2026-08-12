package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class QueryTreeMerger {
    public static Node mergeAndReduce(Node aTree, Node bTree, JsonLd jsonLd) {
        Node merged = aTree instanceof Any
                ? bTree
                : (bTree instanceof Any ? aTree : merge(aTree, bTree, jsonLd));
        // Reduce to avoid duplicates
        return QueryTreeReducer.reduce(merged, jsonLd);
    }

    private static Node merge(Node a, Node b, JsonLd jsonLd) {
        if (a instanceof Or or) {
            return Node.withMappedChildren(or, child -> merge(child, b, jsonLd));
        }

        RdfSubjectType aType = a.rdfSubjectType();

        if (aType.isNoType()) {
            // No type conflict, just merge as is
            return mergeConjunction(a, b, jsonLd);
        }

        if (aType.isMultiType()) {
            return mergeConstrainedByMultipleTypes(a, aType, b, jsonLd);
        }

        return mergeConstrainedBySingleType(a, aType.singleType(), b, jsonLd);
    }

    private static Node mergeConjunction(Node a, Node b, JsonLd jsonLd) {
        List<Node> mergedChildren = new ArrayList<>(a instanceof And and ? and.children() : List.of(a));

        (b instanceof And and ? and.children().stream() : Stream.ofNullable(b))
                .filter(n -> !QueryTreeReducer.implies(a, n.getInverse(), jsonLd))
                .forEach(mergedChildren::add);

        return (mergedChildren.size() == 1 ? mergedChildren.getFirst() : new And(mergedChildren));
    }

    private static Node mergeConstrainedByMultipleTypes(Node a, RdfSubjectType aTypes, Node b, JsonLd jsonLd) {
        // type:(T1 OR T2) X --> (type:T1 X) OR (type:T2 X)
        Or distributed = distributeByType(a, aTypes);
        Node merged = merge(distributed, b, jsonLd);
        // If nothing was merged return the original more compact form
        return merged.equals(distributed) ? a : merged;
    }

    private static Node mergeConstrainedBySingleType(Node a, Type aType, Node b, JsonLd jsonLd) {
        return mergeConjunction(a, adaptToType(aType.type(), b, jsonLd), jsonLd);
    }

    private static Node adaptToType(String type, Node tree, JsonLd jsonLd) {
        if (tree == null) {
            return null;
        }

        if (tree instanceof Or or) {
            return Node.withMappedChildren(or, child -> adaptToType(type, child, jsonLd));
        }

        RdfSubjectType bRdfSubjectType = tree.rdfSubjectType();

        if (bRdfSubjectType.isNoType()) {
            return retainCompatibleByDomain(type, tree, jsonLd);
        }

        if (bRdfSubjectType.isMultiType()) {
            Or distributed = distributeByTypeAndRetainCompatible(tree, bRdfSubjectType, jsonLd);
            return adaptToType(type, distributed, jsonLd);
        }

        Type bSubjectType = bRdfSubjectType.singleType();

        if (jsonLd.isSubClassOf(type, bSubjectType.type())) {
            // Explicit b type given and compatible with a type -> assume that the entire b expression is compatible with a.
            return removeTypeConstraint(tree, bRdfSubjectType.asNode());
        }

        Optional<Property> aToBIntegralRelation = QueryUtil.getIntegralRelationsForType(type, jsonLd)
                .stream()
                .filter(p -> p.range().stream()
                        .anyMatch(r -> jsonLd.isSubClassOf(bSubjectType.type(), r)))
                .findFirst();
        if (aToBIntegralRelation.isPresent()) {
            // Also compatible types, indirectly via integral relation
            Property relation = aToBIntegralRelation.get();
            Condition integralType = bSubjectType.withSelector(new Path(List.of(relation, bSubjectType.rdfTypeProperty())));
            return Node.replace(tree, bSubjectType, integralType);
        }

        // b type incompatible with a type
        return null;
    }

    private static Or distributeByType(Node n, RdfSubjectType nRdfSubjectType) {
        Node noTypeTree = removeTypeConstraint(n, nRdfSubjectType.asNode());
        return new Or(nRdfSubjectType.asList().stream().map(t -> new And(List.of(t, noTypeTree))).toList());
    }

    private static Or distributeByTypeAndRetainCompatible(Node n, RdfSubjectType nRdfSubjectType, JsonLd jsonLd) {
        List<Node> grouped = new ArrayList<>();
        Node noTypeTree = removeTypeConstraint(n, nRdfSubjectType.asNode());
        for (Type t : nRdfSubjectType.asList()) {
            var compatibleInGroup = retainCompatibleByDomain(t.type(), noTypeTree, jsonLd);
            grouped.add(compatibleInGroup == null ? t : new And(List.of(t, compatibleInGroup)));
        }
        return new Or(grouped);
    }

    private static Node removeTypeConstraint(Node tree, Node typeNode) {
        return Node.remove(tree, List.of(typeNode));
    }

    private static Node retainCompatibleByDomain(String rdfSubjectType, Node tree, JsonLd jsonLd) {
        Predicate<Condition> isCompatibleByDomain = c -> c.selector().appearsOnType(rdfSubjectType, jsonLd)
                || c.selector().indirectlyAppearsOnType(rdfSubjectType, jsonLd);

        Predicate<Node> isIncompatible = node -> switch (node) {
            case Condition c -> !isCompatibleByDomain.test(c);
            case Not(Condition c) -> !isCompatibleByDomain.test(c);
            case FilterAlias ignored -> false; // TODO?
            default -> false;
        };

        List<Node> incompatibleNodes = Node.allDescendants(tree).filter(isIncompatible).toList();

        return Node.remove(tree, incompatibleNodes);
    }
}

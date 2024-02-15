package whelk.xlql;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.util.*;

public class SimpleQueryTree {
    public sealed interface Node permits And, Or, PropertyValue, FreeText {}
    public record And (List<Node> conjuncts) implements Node {}
    public record Or (List<Node> disjuncts) implements Node {}
    public record PropertyValue(String property, List<String> propertyPath, Operator operator, String value) implements Node {}
    public record FreeText(Operator operator, String value) implements Node {}

    public Node tree;

    public SimpleQueryTree(FlattenedAst ast, Disambiguate disambiguate) throws InvalidQueryException {
        this.tree = buildTree(ast.tree, disambiguate);
    }

    private static Node buildTree(FlattenedAst.Node ast, Disambiguate disambiguate) throws InvalidQueryException {
        switch (ast) {
            case FlattenedAst.And and -> {
                List<Node> conjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : and.operands()) {
                    conjuncts.add(buildTree(o, disambiguate));
                }
                return new And(conjuncts);
            }
            case FlattenedAst.Or or -> {
                List<Node> disjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : or.operands()) {
                    disjuncts.add(buildTree(o, disambiguate));
                }
                return new Or(disjuncts);
            }
            case FlattenedAst.Not not -> {
                return new FreeText(Operator.NOT_EQUALS, not.value());
            }
            case FlattenedAst.Leaf l -> {
                return new FreeText(Operator.EQUALS, l.value());
            }
            case FlattenedAst.Code c -> {
                String property = null;
                List<String> propertyPath = new ArrayList<>();
                for (String part : c.code().split("\\.")) {
                    if (disambiguate.isLdKey(part)) {
                        propertyPath.add(part);
                        continue;
                    }
                    property = disambiguate.mapToKbvProperty(part);
                    if (property == null) {
                        throw new InvalidQueryException("Unrecognized property alias: " + c);
                    }
                    propertyPath.add(property);
                }

                // TODO: Disambiguate value too
                return new PropertyValue(property, propertyPath, c.operator(), c.value());
            }
        }
    }

    public static Node excludeFromTree(Node nodeToExclude, Node tree) {
        if (nodeToExclude.equals(tree)) {
            return null;
        }
        switch (tree) {
            case And and -> {
                List<Node> andClause = and.conjuncts()
                        .stream()
                        .map(c -> excludeFromTree(nodeToExclude, c))
                        .filter(Objects::nonNull)
                        .toList();
                return andClause.size() > 1 ? new And(andClause) : andClause.get(0);
            }
            case Or or -> {
                List<Node> orClause = or.disjuncts()
                        .stream()
                        .map(d -> excludeFromTree(nodeToExclude, d))
                        .filter(Objects::nonNull)
                        .toList();
                return orClause.size() > 1 ? new Or(orClause) : orClause.get(0);
            }
            case FreeText ignored -> {
                return tree;
            }
            case PropertyValue ignored -> {
                return tree;
            }
        }
    }
}

package whelk.xlql;

import whelk.exception.InvalidQueryException;

import java.util.ArrayList;
import java.util.List;

public class SimpleQueryTree {
    public sealed interface Node permits And, Or, PropertyValue, FreeText {}
    public record And (List<Node> conjuncts) implements Node {}
    public record Or (List<Node> disjuncts) implements Node {}
    public record PropertyValue(String property, Operator operator, String value) implements Node {}
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
                String kbvProperty = disambiguate.mapToKbvProperty(c.code());
                if (kbvProperty == null) {
                    throw new InvalidQueryException("Unrecognized property alias: " + c);
                }
                // TODO: Disambiguate value too
                return new PropertyValue(kbvProperty, c.operator(), c.value());
            }
        }
    }
}

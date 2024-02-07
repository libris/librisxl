package whelk.xlql;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                List<String> disambiguated = Arrays.stream(c.code().split("\\."))
                        // Exclude @id and _str from disambiguation
                        .filter(x -> !Set.of(JsonLd.getID_KEY(), JsonLd.getSEARCH_KEY()).contains(x))
                        .map(disambiguate::mapToKbvProperty)
                        .collect(Collectors.toList());

                if (disambiguated.contains(null)) {
                    throw new InvalidQueryException("Unrecognized property alias: " + c);
                }

                if (c.code().endsWith(JsonLd.getID_KEY())) {
                    disambiguated.add(JsonLd.getID_KEY());
                } else if (c.code().endsWith(JsonLd.getSEARCH_KEY())) {
                    disambiguated.add(JsonLd.getSEARCH_KEY());
                }

                String propertyPath = String.join(".", disambiguated);

                // TODO: Disambiguate value too
                return new PropertyValue(propertyPath, c.operator(), c.value());
            }
        }
    }
}

package whelk.xlql;

import whelk.search.XLQLQuery;

import java.util.*;

public class FlattenedAst {
    public sealed interface Node permits And, Or, Not, Code, Leaf {
    }

    public record And(List<Node> operands) implements Node {
    }

    public record Or(List<Node> operands) implements Node {
    }

    public record Not(String value) implements Node {
    }

    public record Code(String code, Operator operator, String value) implements Node {
    }

    public record Leaf(String value) implements Node {
    }

    private static final Map<String, Operator> OPERATOR_MAPPINGS = operatorMappings();
    private static final Map<Operator, Operator> OPERATOR_OPPOSITES = operatorOpposites();

    Node tree;

    public FlattenedAst(Ast ast) {
        this.tree = flatten(ast);
    }

    static Node flatten(Ast ast) {
        Ast.Node flattenedCodes = flattenCodes(ast.tree);
        Node flattenedNegations = flattenNegations(flattenedCodes);
        return mergeLeaves(flattenedNegations);
    }

    static Node mergeLeaves(Node astNode) {
        return switch (astNode) {
            case And and -> {
                List<Node> newOperands = new ArrayList<>();
                List<String> leafValues = new ArrayList<>();
                for (Node node : and.operands()) {
                    switch (node) {
                        case Leaf l -> leafValues.add(XLQLQuery.quoteIfPhraseOrContainsSpecialSymbol(l.value()));
                        default -> newOperands.add(mergeLeaves(node));
                    }
                }
                if (!leafValues.isEmpty()) {
                    newOperands.addFirst(new Leaf(String.join(" ", leafValues)));
                }
                yield newOperands.size() > 1 ? new And(newOperands) : newOperands.getFirst();
            }
            case Or or -> {
                List<Node> newOperands = or.operands()
                        .stream()
                        .map(FlattenedAst::mergeLeaves)
                        .toList();
                yield new Or(newOperands);
            }
            case Not not -> new Not(XLQLQuery.quoteIfPhraseOrContainsSpecialSymbol(not.value()));
            case Code code -> code;
            case Leaf leaf -> new Leaf(XLQLQuery.quoteIfPhraseOrContainsSpecialSymbol(leaf.value()));
        };
    }

    static Node flattenNegations(Ast.Node ast) {
        return flattenNegations(ast, false);
    }

    private static Node flattenNegations(Ast.Node astNode, boolean negate) {
        switch (astNode) {
            case Ast.And and -> {
                List<Node> operands = new ArrayList<>();
                for (Ast.Node o : and.operands()) {
                    operands.add(flattenNegations(o, negate));
                }
                return negate ? new Or(operands) : new And(operands);
            }
            case Ast.Or or -> {
                List<Node> operands = new ArrayList<>();
                for (Ast.Node o : or.operands()) {
                    operands.add(flattenNegations(o, negate));
                }
                return negate ? new And(operands) : new Or(operands);
            }
            case Ast.Not not -> {
                return flattenNegations(not.operand(), !negate);
            }
            case Ast.CodeEqualsLeaf cel -> {
                return new Code(cel.code(), negate ? Operator.NOT_EQUALS : Operator.EQUALS, cel.operand().value());
            }
            case Ast.CodeLesserGreaterThan clgt -> {
                Operator operator = Optional.of(OPERATOR_MAPPINGS.get(clgt.operator()))
                        .map(o -> negate ? OPERATOR_OPPOSITES.get(o) : o)
                        .get();
                return new Code(clgt.code(), operator, clgt.operand().value());
            }
            case Ast.Leaf l -> {
                return negate ? new Not(l.value()) : new Leaf(l.value());
            }
            case Ast.CodeEquals ignored ->
                    throw new RuntimeException("CodeEquals not allowed here. Run flattenCodes to eliminate this from the AST");
        }
    }

    static Ast.Node flattenCodes(Ast.Node astNode) {
        switch (astNode) {
            // If a CodeEquals is found, recreate all children with the code
            case Ast.CodeEquals ce -> {
                String code = ce.code();
                switch (ce.operand()) {
                    case Ast.And and -> {
                        List<Ast.Node> newOperands = new ArrayList<>();
                        for (Ast.Node o : and.operands()) {
                            newOperands.add(wrapAllChildrenInCode(code, o));
                        }
                        return new Ast.And(newOperands);
                    }
                    case Ast.Or or -> {
                        List<Ast.Node> newOperands = new ArrayList<>();
                        for (Ast.Node o : or.operands()) {
                            newOperands.add(wrapAllChildrenInCode(code, o));
                        }
                        return new Ast.Or(newOperands);
                    }
                    case Ast.Not not -> {
                        return new Ast.Not(wrapAllChildrenInCode(code, not.operand()));
                    }
                    case Ast.Leaf l -> {
                        return wrapAllChildrenInCode(code, l);
                    }
                    default ->
                            throw new RuntimeException("XLQL Error when flattening: " + astNode); // Should not be reachable. This is a bug.
                }
            }
            // Until a CodeEquals is found, recreate as is
            case Ast.And and -> {
                List<Ast.Node> newOperands = new ArrayList<>();
                for (Ast.Node o : and.operands()) {
                    newOperands.add(flattenCodes(o));
                }
                return new Ast.And(newOperands);
            }
            case Ast.Or or -> {
                List<Ast.Node> newOperands = new ArrayList<>();
                for (Ast.Node o : or.operands()) {
                    newOperands.add(flattenCodes(o));
                }
                return new Ast.Or(newOperands);
            }
            case Ast.Not not -> {
                return new Ast.Not(flattenCodes(not.operand()));
            }
            default -> {
                return astNode;
            }
        }
    }

    private static Ast.Node wrapAllChildrenInCode(String code, Ast.Node astNode) {
        switch (astNode) {
            case Ast.And and -> {
                List<Ast.Node> replacementOperands = new ArrayList<>();
                for (Ast.Node child : and.operands()) {
                    replacementOperands.add(wrapAllChildrenInCode(code, child));
                }
                return new Ast.And(replacementOperands);
            }
            case Ast.Or or -> {
                List<Ast.Node> replacementOperands = new ArrayList<>();
                for (Ast.Node child : or.operands()) {
                    replacementOperands.add(wrapAllChildrenInCode(code, child));
                }
                return new Ast.Or(replacementOperands);
            }
            case Ast.Not not -> {
                return new Ast.Not(wrapAllChildrenInCode(code, not.operand()));
            }
            case Ast.Leaf l -> {
                return new Ast.CodeEqualsLeaf(code, l);
            }
            default ->
                    throw new RuntimeException("XLQL Error when flattening: " + astNode); // Should not be reachable. This is a bug.
        }
    }

    private static Map<String, Operator> operatorMappings() {
        return Map.of(
                ">", Operator.GREATER_THAN,
                ">=", Operator.GREATER_THAN_OR_EQUALS,
                "<", Operator.LESS_THAN,
                "<=", Operator.LESS_THAN_OR_EQUALS
        );
    }

    private static Map<Operator, Operator> operatorOpposites() {
        return Map.of(
                Operator.LESS_THAN_OR_EQUALS, Operator.GREATER_THAN,
                Operator.LESS_THAN, Operator.GREATER_THAN_OR_EQUALS,
                Operator.GREATER_THAN_OR_EQUALS, Operator.LESS_THAN,
                Operator.GREATER_THAN, Operator.LESS_THAN_OR_EQUALS
        );
    }
}

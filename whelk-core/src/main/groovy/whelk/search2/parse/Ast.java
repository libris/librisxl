package whelk.search2.parse;

import whelk.exception.InvalidQueryException;
import whelk.search2.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Ast {
    public sealed interface Node permits Group, Code, Not, Leaf {
    }

    public sealed interface Group extends Node permits And, Or {
        List<Node> operands();

        Group newInstance(List<Node> operands);

        default List<Node> mapChildren(Function<Node, Node> mapper) {
            return operands().stream().map(mapper).toList();
        }
    }

    public record And(List<Node> operands) implements Group {
        @Override
        public Group newInstance(List<Node> operands) {
            return new And(operands);
        }
    }

    public record Or(List<Node> operands) implements Group {
        @Override
        public Group newInstance(List<Node> operands) {
            return new Or(operands);
        }
    }

    public record Not(Node operand) implements Node {
    }

    public record Code(String code, Operator operator, Node operand) implements Node {
        private boolean isEquals() {
            return operator.equals(Operator.EQUALS);
        }
    }

    //    public record Like (Node operand) implements Node {}

    public record Leaf(String value) implements Node {
    }

    public Node tree;

    public Ast(Parse.OrComb orComb) throws InvalidQueryException {
        this.tree = flatten(buildFrom(orComb));
    }

    public static Node buildFrom(Parse.OrComb orComb) throws InvalidQueryException {
        Node ast = reduce(orComb);
        Analysis.checkSemantics(ast);
        return ast;
    }

    private static Node reduce(Parse.OrComb orComb) throws InvalidQueryException {
        // public record OrComb(List<AndComb> andCombs) {}

        if (orComb.andCombs().size() > 1) {
            List<Node> result = new ArrayList<>();
            for (Parse.AndComb andComb : orComb.andCombs()) {
                result.add(reduce(andComb));
            }
            return new Or(result);
        } else {
            return reduce(orComb.andCombs().getFirst());
        }
    }

    private static Node reduce(Parse.AndComb andComb) throws InvalidQueryException {
        // public record AndComb(List<Term> ts) {}

        if (andComb.ts().size() > 1) {
            List<Node> result = new ArrayList<>();
            for (Parse.Term t : andComb.ts()) {
                result.add(reduce(t));
            }
            return new And(result);
        } else {
            return reduce(andComb.ts().getFirst());
        }
    }

    private static Node reduce(Parse.Term term) throws InvalidQueryException {
        // public record Term (String string1, Uoperator uop, Term term, Group group, Boperator bop, BoperatorEq bopeq, String string2) {}

        // Ordinary string term
        if (term.string1() != null &&
                term.uop() == null &&
                term.term() == null &&
                term.group() == null &&
                term.bop() == null &&
                term.bopeq() == null &&
                term.string2() == null) {
            return new Leaf(term.string1());
        }

        // Group term
        else if (term.string1() == null &&
                term.uop() == null &&
                term.term() == null &&
                term.group() != null &&
                term.bop() == null &&
                term.bopeq() == null &&
                term.string2() == null) {
            return reduce(term.group());
        }

        // Operator and term (for example NOT AAA)
        else if (term.string1() == null &&
                term.uop() != null &&
                term.term() != null &&
                term.group() == null &&
                term.bop() == null &&
                term.bopeq() == null &&
                term.string2() == null) {

            if (term.uop().s().equals("!") || term.uop().s().equals("not")) {
                return new Not(reduce(term.term()));
            } else if (term.uop().s().equals("~")) {
                throw new InvalidQueryException("Like operator (~) not yet supported");
//                return new Like(reduce(term.term()));
            }
        }

        // Code equals (:/=) term
        else if (term.string1() == null &&
                term.uop() == null &&
                term.term() != null &&
                term.group() == null &&
                term.bop() == null &&
                term.bopeq() != null &&
                term.string2() != null) {
            return new Code(term.string2(), Operator.EQUALS, reduce(term.term()));
        }

        // Code less/greater than
        else if (term.string1() != null &&
                term.uop() == null &&
                term.term() == null &&
                term.group() == null &&
                term.bop() != null &&
                term.bopeq() == null &&
                term.string2() != null) {
            return new Code(term.string2(), Operator.symbolMappings().get(term.bop().op()), new Leaf(term.string1()));
        }

        throw new RuntimeException("XLQL Error when reducing: " + term); // Should not be reachable. This is a bug.
    }

    private static Node reduce(Parse.Group group) throws InvalidQueryException {
        // public record Group(OrComb o, AndComb a, Group g) {}
        if (group.g() != null) {
            return reduce(group.g());
        } else if (group.a() != null) {
            return reduce(group.a());
        } else if (group.o() != null) {
            return reduce(group.o());
        }

        throw new RuntimeException("XLQL Error when reducing: " + group); // Should not be reachable. This is a bug.
    }

    private static Node flatten(Node tree) {
        return flattenNegations(flattenCodes(tree));
    }

    static Node flattenCodes(Node node) {
        return switch (node) {
            // If Code with equals operator, recreate all children with the code
            case Code c -> !c.isEquals() ? c : switch (c.operand()) {
                case Group g -> g.newInstance(g.mapChildren(o -> wrapAllChildrenInCode(c.code(), o)));
                case Not not -> new Not(wrapAllChildrenInCode(c.code(), not.operand()));
                case Leaf l -> wrapAllChildrenInCode(c.code(), l);
                case Code ignored ->
                        throw new RuntimeException("Error when flattening: " + node); // Should not be reachable. This is a bug.
            };
            // Until a Code with equals operator is found, recreate as is
            case Group g -> g.newInstance(g.mapChildren(Ast::flattenCodes));
            case Not not -> new Not(flattenCodes(not.operand()));
            case Leaf leaf -> leaf;
        };
    }

    private static Node wrapAllChildrenInCode(String code, Node node) {
        return switch (node) {
            case Group g -> g.newInstance(g.mapChildren(c -> wrapAllChildrenInCode(code, c)));
            case Not not -> new Not(wrapAllChildrenInCode(code, not.operand()));
            case Leaf leaf -> new Code(code, Operator.EQUALS, leaf);
            case Code ignored ->
                    throw new RuntimeException("Error when flattening: " + node); // Should not be reachable. This is a bug.
        };
    }

    static Node flattenNegations(Node node) {
        return flattenNegations(node, false);
    }

    static Node flattenNegations(Node node, boolean negate) {
        return switch (node) {
            case And and -> {
                List<Node> newChildren = and.mapChildren(c -> flattenNegations(c, negate));
                yield negate ? new Or(newChildren) : new And(newChildren);
            }
            case Or or -> {
                List<Node> newChildren = or.mapChildren(c -> flattenNegations(c, negate));
                yield negate ? new And(newChildren) : new Or(newChildren);
            }
            case Code code -> {
                if (code.operand() instanceof Leaf) {
                    yield negate ? new Code(code.code(), code.operator().getInverse(), code.operand()) : code;
                }
                throw new RuntimeException("Error: Operand must be nothing but Leaf. Run flattenCodes to avoid this.");
            }
            case Not not -> flattenNegations(not.operand(), !negate);
            case Leaf leaf -> negate ? new Not(leaf) : leaf;
        };
    }
}

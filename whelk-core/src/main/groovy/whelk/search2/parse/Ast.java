package whelk.search2.parse;

import whelk.exception.InvalidQueryException;
import whelk.search2.Operator;

import java.util.ArrayList;
import java.util.List;

public class Ast {
    public sealed interface Node permits Group, Code, Not, Leaf {
    }

    public sealed interface Group extends Node permits And, Or {
        List<Node> operands();
    }

    public record And(List<Node> operands) implements Group {
    }

    public record Or(List<Node> operands) implements Group {
    }

    public record Not(Node operand) implements Node {
    }

    public record Code(Lex.Symbol code, Operator operator, Node operand) implements Node {
    }

    //    public record Like (Node operand) implements Node {}

    public record Leaf(Lex.Symbol value) implements Node {
    }

    public Node tree;

    public Ast(Parse.OrComb orComb) throws InvalidQueryException {
        this.tree = buildFrom(orComb);
    }

    public static Node buildFrom(Parse.OrComb orComb) throws InvalidQueryException {
        Node ast = reduce(orComb);
        Analysis.checkSemantics(ast);
        return ast;
    }

    private static Node reduce(Parse.OrComb orComb) throws InvalidQueryException {
        // public record OrComb(List<AndComb> andCombs) {}

        return switch (orComb.andCombs().size()) {
            case 0 -> new Or(List.of());
            case 1 -> reduce(orComb.andCombs().getFirst());
            default -> {
                List<Node> result = new ArrayList<>();
                for (Parse.AndComb andComb : orComb.andCombs()) {
                    result.add(reduce(andComb));
                }
                yield new Or(result);
            }
        };
    }

    private static Node reduce(Parse.AndComb andComb) throws InvalidQueryException {
        // public record AndComb(List<Term> ts) {}

        return switch (andComb.ts().size()) {
            case 0 -> new And(List.of());
            case 1 -> reduce(andComb.ts().getFirst());
            default -> {
                List<Node> result = new ArrayList<>();
                for (Parse.Term t : andComb.ts()) {
                    result.add(reduce(t));
                }
                yield new And(result);
            }
        };
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

            if (term.uop().s().value().equals("!") || term.uop().s().value().equals("not")) {
                return new Not(reduce(term.term()));
            } else if (term.uop().s().value().equals("~")) {
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
            return new Code(term.string2(), Operator.symbolMappings().get(term.bop().op().value()), new Leaf(term.string1()));
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
}

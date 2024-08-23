package whelk.search2.parse;

import whelk.exception.InvalidQueryException;

import java.util.ArrayList;
import java.util.List;

public class Ast {
    public sealed interface Node permits And, Or, Not, CodeEquals, CodeLesserGreaterThan, CodeEqualsLeaf, Leaf {
    }

    public record And(List<Node> operands) implements Node {
    }

    public record Or(List<Node> operands) implements Node {
    }

    public record Not(Node operand) implements Node {
    }

    //    public record Like (Node operand) implements Node {}
    public record CodeEquals(String code, Node operand) implements Node {
    }

    public record CodeEqualsLeaf(String code, Leaf operand) implements Node {
    }

    public record CodeLesserGreaterThan(String code, String operator, Leaf operand) implements Node {
    }

    public record Leaf(String value) implements Node {
    }

    Node tree;

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
            return new CodeEquals(term.string2(), reduce(term.term()));
        }

        // Code less/greater than
        else if (term.string1() != null &&
                term.uop() == null &&
                term.term() == null &&
                term.group() == null &&
                term.bop() != null &&
                term.bopeq() == null &&
                term.string2() != null) {
            return new CodeLesserGreaterThan(term.string2(), term.bop().op(), new Leaf(term.string1()));
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

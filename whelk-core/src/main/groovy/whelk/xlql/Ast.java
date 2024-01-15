package whelk.xlql;

import java.util.ArrayList;
import java.util.List;

public class Ast {
    public record And (List<Object> operands) {}
    public record Or (List<Object> operands) {}
    public record Not (Object operand) {}
    public record Like (Object operand) {}
    public record CodeEquals (String code, Object operand) {}
    public record CodeLesserGreaterThan (String code, String operator, String operand) {}

    public static Object buildFrom(Parse.OrComb orComb) {
        return reduce(orComb);
    }

    private static Object reduce(Parse.OrComb orComb) {
        // public record OrComb(List<AndComb> andCombs) {}

        if (orComb.andCombs().size() > 1) {
            List<Object> result = new ArrayList<>();
            for (Parse.AndComb andComb : orComb.andCombs()) {
                result.add(reduce(andComb));
            }
            return new Or(result);
        }
        else {
            return reduce(orComb.andCombs().get(0));
        }
    }

    private static Object reduce(Parse.AndComb andComb) {
        // public record AndComb(List<Term> ts) {}

        if (andComb.ts().size() > 1) {
            List<Object> result = new ArrayList<>();
            for (Parse.Term t : andComb.ts()) {
                result.add(reduce(t));
            }
            return new And(result);
        } else {
            return reduce(andComb.ts().get(0));
        }
    }

    private static Object reduce (Parse.Term term) {
        // public record Term (String string1, Uoperator uop, Term term, Group group, Boperator bop, BoperatorEq bopeq, String string2) {}

        // Ordinary string term
        if (term.string1() != null &&
                term.uop() == null &&
                term.term() == null &&
                term.group() == null &&
                term.bop() == null &&
                term.bopeq() == null &&
                term.string2() == null) {
            return term.string1();
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

            if ( term.uop().s().equals("!") || term.uop().s().equals("not")) {
                return new Not(reduce(term.term()));
            } else if ( term.uop().s().equals("~") ) {
                return new Like(reduce(term.term()));
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
            return new CodeLesserGreaterThan(term.string2(), term.bop().op(), term.string1());
        }

        throw new RuntimeException("XLQL Error when reducing: " + term); // Should not be reachable. This is a bug.
    }

    private static Object reduce(Parse.Group group) {
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

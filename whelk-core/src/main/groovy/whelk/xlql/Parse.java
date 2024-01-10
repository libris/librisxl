package whelk.xlql;

import java.util.*;
import java.util.function.Function;

public class Parse
{
    /**
     * Grammar:
     *
     * QUERY: ( PART )*
     * PART: ORCOMB | ANDCOMB | GROUP
     * GROUP: "(" ORCOMB | ANDCOMB | GROUP ")"
     * ORCOMB: ANDCOMB ( "OR" ANDCOMB )*
     * ANDCOMB: TERM ( "AND" TERM | TERM )*
     * TERM: STRING | UOPERATOR TERM | UOPERATOR GROUP
     * UOPERATOR: "!" | "~" | "NOT" | CODE
     * CODE: [STRING ending in ":"]
     * STRING: ...
     *
     */

    public enum OPERATORKEYWORD {
        AND,
        OR,
        NOT,
    }

    public record Group() {} // TODO
    public record AndComb(List<Term> ts) {} // TODO
    public record Term (String s, Uoperator uop, Term t, Group g) {}
    public record Code (String s) {}
    public record Uoperator (String s, Code c) {}

    public static void parseQuery(LinkedList<Lex.Symbol> symbols) {
        LinkedList<Object> stack = new LinkedList<>();
        while (!symbols.isEmpty()) {
            shift(stack, symbols);
            boolean reductionWasPossible;
            do {
                reductionWasPossible = reduce(stack);
            }
            while(reductionWasPossible);
        }
        // SPECIAL IDEA FOR HANDLING * (lists), do them "second order", only when there are no more reductions (OR SHIFTS!) possible.
        // Actually just flip the input order and consider it the stack. No need for shifting. Just reduce right to left

        System.out.println("Parse termination.");
    }

    // Note to self, the front of the list counts as the top!
    private static void shift(LinkedList<Object> stack, LinkedList<Lex.Symbol> symbols) {
        stack.push( symbols.poll() );
        System.out.print("Stack and next symbols:\n\t stack: " + stack);
        if (!symbols.isEmpty())
            System.out.println("\n\t next: " + symbols.getFirst());
        else
            System.out.println();
    }

    private static boolean reduce(LinkedList<Object> stack) {

        // CODE: [STRING ending in ":"] // OK FOR NOW BUT INCORRECT! "CODE" NEEDS TO BE LEXED SEPARATELY, NEVER A STRING
        {
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING &&
                        s.value().endsWith(":")) {
                    stack.pop();
                    stack.push(new Code(s.value()));
                    return true;
                }
            }
        }

        // UOPERATOR: "!" | "~" | "NOT" | CODE
        {
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING &&
                        ( s.value().equals("!") || s.value().equals("~") ) ) {
                    stack.pop();
                    stack.push(new Uoperator(s.value(), null));
                    return true;
                }
                else if (stack.get(0) instanceof Code c) {
                    stack.pop();
                    stack.push(new Uoperator(null, c));
                    return true;
                }
            }
        }

        // TERM: STRING | UOPERATOR TERM | UOPERATOR GROUP
        {
            if (stack.size() >= 2) {
                if (stack.get(0) instanceof Uoperator uop) {
                    if (stack.get(1) instanceof Term t) {
                        stack.pop();
                        stack.pop();
                        stack.push(new Term(null, uop, t, null));
                    }
                }
            }
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new Term(s.value(), null, null, null));
                    return true;
                }
            }
        }

        // ANDCOMB: TERM ( "AND" TERM | TERM )*
        {
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Term t) {

                    List<Term> terms = new ArrayList<>();
                    terms.add(t);
                    stack.pop();
                    stack.push(new AndComb(terms));

                    boolean stillChewing = false;
                    do {
                        if (stack.get(0) instanceof Term nextTerm) {
                            stack.pop();
                            terms.add(nextTerm);
                            stillChewing = true;
                        } else if (stack.get(0) instanceof Lex.Symbol s &&
                                s.name() == Lex.TokenName.KEYWORD &&
                                s.value().equals("and") &&
                                stack.get(1) instanceof Term nextTerm) {
                            stack.pop();
                            stack.pop();
                            terms.add(nextTerm);
                            stillChewing = true;
                        }
                    } while (stillChewing);
                }
            }
        }

        return false;
    }
}
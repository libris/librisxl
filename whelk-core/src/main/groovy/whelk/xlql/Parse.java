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
        UNION,
    }

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

    public record Term (String s) {}
    public record Code (String s) {}


    private static boolean reduce(LinkedList<Object> stack) {

        // term
        {
            Object top = stack.peek();
            if (top instanceof Lex.Symbol symbol) {
                if (symbol.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new Term(symbol.value()));
                    return true;
                }
            }
        }

        // code
        {
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Lex.Symbol s && s.value().endsWith(":")) {
                    stack.pop();
                    stack.push(new Code(s.value()));
                    return true;
                }
            }
        }

        /*// modifier 	::= 	'/' term comparisonList
        {
            if (stack.size() >= 3) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        stack.get(1) instanceof Term t &&
                        stack.get(2) instanceof ComparisonList l) {
                    if (s.name() == Lex.TokenName.OPERATOR && s.value().equals("/")) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new Modifier(t, l));
                    }
                }
            }
        }*/

        return false;
    }
}
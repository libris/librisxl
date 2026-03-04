package whelk.sru.cql;

/**
 *  This class is an adaptation of whelk.search2.parse.Parse, but modified for the pure CQL case.
 */


import whelk.exception.InvalidQueryException;
import whelk.sru.cql.Lex;

import java.util.*;

public class Parse {

    /**
     * CQL (1.1) equivalent BNF (modified without changed semantics to not be broken):
     *
     * cqlQuery 	::= 	prefixAssignment cqlQuery | scopedClause
     * prefixAssignment 	::= 	'>' term '=' term | '>' term
     * scopedClause 	::= 	scopedClause booleanGroup searchClause | searchClause
     * booleanGroup 	::= 	boolean [modifierList]
     * boolean 	::= 	'and' | 'or' | 'not' | 'prox'
     * searchClause 	::= 	'(' cqlQuery ')' | term relation term | term
     * relation 	::= 	comparitor [modifierList]
     * comparitor 	::= 	comparitorSymbol | namedComparitor
     * comparitorSymbol 	::= 	'=' | '>' | '<' | '>=' | '<=' | '<>' | '=='
     * namedComparitor 	::= 	identifier
     * modifierList 	::= 	modifierList modifier | modifier
     * modifier 	::= 	'/' term [comparitorSymbol term]
     * term 	::= 	identifier | 'and' | 'or' | 'not' | 'prox' | 'sortby'
     * identifier 	::= 	charString1 | charString2
     */

    // helper (not part of grammar)
    //public record Pair(Object c, Object t) {} // [comparitorSymbol term]

    // CST nodes
    public record CqlQuery() {}
    public record Identifier(Lex.Symbol s) {}
    public record Term(Identifier i, Lex.Symbol k) {}
    public record Modifier(Term t1, Lex.Symbol s, Term t2) {}


    public static CqlQuery parseQuery(LinkedList<Lex.Symbol> symbols) throws InvalidQueryException {
        LinkedList<Object> stack = new LinkedList<>();
        while (!symbols.isEmpty()) {
            shift(stack, symbols);
            boolean reductionWasPossible;
            do {
                Lex.Symbol lookahead = null;
                if (!symbols.isEmpty())
                    lookahead = symbols.getFirst();
                reductionWasPossible = reduce(stack, lookahead);


                System.out.println("After reduction, stack and next symbols:\n\tstack:");//\n\t stack: " + stack);
                for (Object o : stack) {
                    System.out.println("\t\t"+o.toString());
                }
                if (!symbols.isEmpty())
                    System.out.println("\t next: " + lookahead + "\n");
                else
                    System.out.println();


            }
            while (reductionWasPossible);
        }

        if (stack.size() == 1 && stack.getFirst() instanceof CqlQuery) {
            return (CqlQuery) stack.getFirst();
        }

        throw new InvalidQueryException("Syntax error");
    }

    // Note to self, the front of the list counts as the top!
    private static void shift(LinkedList<Object> stack, LinkedList<Lex.Symbol> symbols) {
        stack.push(symbols.poll());
    }

    private static boolean reduce(LinkedList<Object> stack, Lex.Symbol lookahead) {

        // TEMP! TEST THE TESTING! NOT REAL PARSING!
        /*{
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Term) {
                    stack.pop();
                    stack.push(new CqlQuery());
                    return true;
                }
            }
        }*/
        //------------------------------------------

        // modifier 	::= 	'/' term [comparitorSymbol term]
        {
            List<String> comparitorSymbols = List.of("=", ">", "<", ">=", "<=", "<>", "=="); // Not optimal, should perhaps be defined externally for performance
            boolean lookaheadIsCompSymb = lookahead instanceof Lex.Symbol la && la.name() == Lex.TokenName.OPERATOR && comparitorSymbols.contains(la.value());
            if (
                    stack.size() == 4 &&
                    stack.get(0) instanceof Term t2 &&
                    stack.get(1) instanceof Lex.Symbol c &&
                    c.name() == Lex.TokenName.OPERATOR &&
                    comparitorSymbols.contains( c.value() ) &&
                    stack.get(2) instanceof Term t1 &&
                    stack.get(3) instanceof Lex.Symbol s &&
                    s.name() == Lex.TokenName.OPERATOR &&
                    s.value().equals("/")) {
                stack.pop();
                stack.pop();
                stack.pop();
                stack.pop();
                stack.push(new Modifier(t1, c, t2));
            } else if (
                    stack.size() == 2 &&
                    stack.get(0) instanceof Term t &&
                    stack.get(1) instanceof Lex.Symbol s &&
                    s.name() == Lex.TokenName.OPERATOR &&
                    s.value().equals("/") &&
                    !lookaheadIsCompSymb
            ) {
                stack.pop();
                stack.pop();
                stack.push(new Modifier(t, null, null));
            }
        }

        // term 	::= 	identifier | 'and' | 'or' | 'not' | 'prox'
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Identifier i) {
                    stack.pop();
                    stack.push(new Term(i, null));
                    return true;
                }
                else if (stack.getFirst() instanceof Lex.Symbol s && s.name() == Lex.TokenName.KEYWORD) {
                    stack.pop();
                    stack.push(new Term(null, s));
                    return true;
                }
            }
        }

        // identifier 	::= 	charString1 | charString2
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new Identifier(s));
                    return true;
                }
            }
        }

        return false;
    }
}
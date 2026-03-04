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

    final static List<String> comparitorSymbols = List.of("=", ">", "<", ">=", "<=", "<>", "=="); // Not optimal, should perhaps be defined externally for performance

    // CST nodes
    public record CqlQuery() {}
    public record Identifier(Lex.Symbol s) {}
    public record Term(Identifier i, Lex.Symbol k) {}
    public record Modifier(Term t1, ComparitorSymbol c, Term t2) {}
    public record ModifierList(List<Modifier> l) {}
    public record ComparitorSymbol(Lex.Symbol s) {}


    public static CqlQuery parseQuery(LinkedList<Lex.Symbol> symbols) throws InvalidQueryException {
        LinkedList<Object> stack = new LinkedList<>();
        while (!symbols.isEmpty()) {
            shift(stack, symbols);
            boolean reductionWasPossible;
            do {
                Lex.Symbol lookahead = null;
                if (!symbols.isEmpty())
                    lookahead = symbols.get(0);
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

        if (stack.size() == 1 && stack.get(0) instanceof CqlQuery) {
            return (CqlQuery) stack.get(0);
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
                if (stack.get(0) instanceof Term) {
                    stack.pop();
                    stack.push(new CqlQuery());
                    return true;
                }
            }
        }*/
        //------------------------------------------

        // modifierList 	::= 	modifierList modifier | modifier
        {
            if (stack.size() >= 2 && stack.get(0) instanceof Modifier m && stack.get(1) instanceof ModifierList l) {
                stack.pop();
                l.l.add(m);
                return true;
            } else if (stack.size() == 1 && stack.get(0) instanceof Modifier m) {
                stack.pop();
                ArrayList<Modifier> l = new ArrayList<>(1);
                l.add(m);
                stack.push(new ModifierList(l));
                return true;
            }
        }

        // modifier 	::= 	'/' term [comparitorSymbol term]
        {
            boolean lookaheadIsCompSymb = lookahead instanceof Lex.Symbol la && la.name() == Lex.TokenName.OPERATOR && comparitorSymbols.contains(la.value());
            if (
                    stack.size() >= 4 &&
                    stack.get(0) instanceof Term t2 &&
                    stack.get(1) instanceof ComparitorSymbol c &&
                    stack.get(2) instanceof Term t1 &&
                    stack.get(3) instanceof Lex.Symbol s &&
                    s.name() == Lex.TokenName.OPERATOR &&
                    s.value().equals("/")) {
                stack.pop();
                stack.pop();
                stack.pop();
                stack.pop();
                stack.push(new Modifier(t1, c, t2));
                return true;
            } else if (
                    stack.size() >= 2 &&
                    stack.get(0) instanceof Term t &&
                    stack.get(1) instanceof Lex.Symbol s &&
                    s.name() == Lex.TokenName.OPERATOR &&
                    s.value().equals("/") &&
                    !lookaheadIsCompSymb
            ) {
                stack.pop();
                stack.pop();
                stack.push(new Modifier(t, null, null));
                return true;
            }
        }

        // term 	::= 	identifier | 'and' | 'or' | 'not' | 'prox'
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Identifier i) {
                    stack.pop();
                    stack.push(new Term(i, null));
                    return true;
                }
                else if (stack.get(0) instanceof Lex.Symbol s && s.name() == Lex.TokenName.KEYWORD) {
                    stack.pop();
                    stack.push(new Term(null, s));
                    return true;
                }
            }
        }

        // identifier 	::= 	charString1 | charString2
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new Identifier(s));
                    return true;
                }
            }
        }

        // comparitorSymbol 	::= 	'=' | '>' | '<' | '>=' | '<=' | '<>' | '=='
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.OPERATOR && comparitorSymbols.contains(s.value())) {
                    stack.pop();
                    stack.push(new ComparitorSymbol(s));
                    return true;
                }
            }
        }

        return false;
    }
}
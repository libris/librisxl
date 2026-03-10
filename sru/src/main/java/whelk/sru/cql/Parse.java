package whelk.sru.cql;

/**
 *  This class is an adaptation of whelk.search2.parse.Parse, but modified for the pure CQL case.
 */


import whelk.exception.InvalidQueryException;
import whelk.sru.cql.Lex;

import java.util.*;

public class Parse {

    /**
     * CQL (1.1) more-or-less equivalent BNF.
     *
     * cqlQuery 	::= 	prefixAssignment cqlQuery | scopedClause
     * prefixAssignment 	::= 	'>' term '=' term | '>' term
     * scopedClause 	::= 	scopedClause booleanGroup searchClause | searchClause
     * booleanGroup 	::= 	boolean [modifierList]
     * boolean 	::= 	'and' | 'or' | 'not' | 'prox'
     * searchClause 	::= 	'(' cqlQuery ')' | term comparitor [modifierList] term | term
     * comparitor 	::= 	comparitorSymbol | term
     * comparitorSymbol 	::= 	'=' | '>' | '<' | '>=' | '<=' | '<>' | '=='
     * modifierList 	::= 	modifierList modifier | modifier
     * modifier 	::= 	'/' term [comparitorSymbol term]
     * term 	::= 	...
     */

    final static List<String> comparitorSymbols = List.of("=", ">", "<", ">=", "<=", "<>", "==");

    // CST nodes
    public record CqlQuery(ScopedClause sc) {}
    public record Term(Lex.Symbol s) {}
    public record Modifier(Term t1, ComparitorSymbol c, Term t2) {}
    public record ModifierList(List<Modifier> l) {}
    public record ComparitorSymbol(Lex.Symbol s) {}
    public record Comparitor(ComparitorSymbol c, Term t) {}
    public record SearchClause(CqlQuery q, Term t1, Comparitor c, ModifierList l, Term t2) {}
    public record Boolean(Lex.Symbol s) {}
    public record BooleanGroup(Boolean b, ModifierList l) {}
    public record ScopedClause(ScopedClause scoped, BooleanGroup bg, SearchClause search) {}


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

        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof ScopedClause sc && !(lookahead instanceof Lex.Symbol s && s.name() == Lex.TokenName.KEYWORD)) {
                    stack.pop();
                    stack.push(new CqlQuery(sc));
                    return true;
                }
            }
        }

        // scopedClause 	::= 	scopedClause booleanGroup searchClause | searchClause
        {
            if (stack.size() >= 3) {
                if (
                        stack.get(0) instanceof SearchClause searchClause &&
                        stack.get(1) instanceof BooleanGroup bg &&
                        stack.get(2) instanceof ScopedClause scopedClause) {
                    stack.pop();
                    stack.pop();
                    stack.pop();
                    stack.push(new ScopedClause(scopedClause, bg, searchClause));
                    return true;
                }
            }
            if (stack.size() >= 1 && stack.get(0) instanceof SearchClause searchClause ) {
                stack.pop();
                stack.push(new ScopedClause(null, null, searchClause));
                return true;
            }
        }

        // booleanGroup 	::= 	boolean [modifierList]
        {
            if (stack.size() >= 2) {
                if (stack.get(0) instanceof ModifierList l && stack.get(1) instanceof Boolean b) {
                    stack.pop();
                    stack.pop();
                    stack.push(new BooleanGroup(b, l));
                    return true;
                }
            }
            if (stack.size() >= 1 && stack.get(0) instanceof Boolean b && !(lookahead instanceof Lex.Symbol s && s.name() == Lex.TokenName.OPERATOR && s.value().equals("/"))) {
                stack.pop();
                stack.push(new BooleanGroup(b, null));
                return true;
            }
        }

        // searchClause 	::= 	'(' cqlQuery ')' | term comparitor [modifierList] term | term
        {
            // 1st
            if (stack.size() >= 2) {
                if (
                        stack.get(0) instanceof Lex.Symbol s1 && s1.name() == Lex.TokenName.OPERATOR && s1.value().equals(")") &&
                        stack.get(1) instanceof CqlQuery q &&
                        stack.get(2) instanceof Lex.Symbol s2 && s2.name() == Lex.TokenName.OPERATOR && s2.value().equals("(")
                ) {
                    stack.pop();
                    stack.pop();
                    stack.pop();
                    stack.push(new SearchClause(q, null, null, null, null));
                    return true;
                }
            }

            // 2nd
            if (stack.size() >= 3) {
                if (stack.get(0) instanceof Term t2) {
                    if (
                            stack.get(1) instanceof ModifierList l &&
                            stack.get(2) instanceof Comparitor c &&
                            stack.get(3) instanceof Term t1 ) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new SearchClause(null, t1, c, l, t2));
                        return true;
                    } else if (
                            stack.get(1) instanceof Comparitor c &&
                            stack.get(2) instanceof Term t1) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new SearchClause(null, t1, c, null, t2));
                        return true;
                    }
                }
            }

            //3rd
            if (stack.size() >= 1) {
                if (stack.get(0) instanceof Term t) {
                    // Must be followed by a non-string or EOF, or this could be a larger search clause waiting to happen.
                    if (lookahead == null || lookahead instanceof Lex.Symbol s && s.name() != Lex.TokenName.STRING) {
                        stack.pop();
                        stack.push(new SearchClause(null, t, null, null, null));
                        return true;
                    }
                }
            }
        }

        // comparitor 	::= 	comparitorSymbol | term - 'and' - 'or' - 'not' - 'prox'
        {
            if (
                    stack.size() >= 2 && stack.get(0) instanceof Term t &&
                    stack.get(1) instanceof Term // This term will not be popped, but we need check for it to distinguish between the comparitor and searchClause cases (this f*****g grammar)
            ) {
                stack.pop();
                stack.push(new Comparitor(null, t));
                return true;
            }
            else if (stack.size() >= 1 && stack.get(0) instanceof ComparitorSymbol c) {
                stack.pop();
                stack.push(new Comparitor(c, null));
                return true;
            }
        }

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

        // term 	::= 	...
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new Term(s));
                    return true;
                }
            }
        }

        // boolean 	::= 	'and' | 'or' | 'not' | 'prox'
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.KEYWORD) {
                    stack.pop();
                    stack.push(new Boolean(s));
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
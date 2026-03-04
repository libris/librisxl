package whelk.sru.cql;

/**
 *  This class is an adaptation of whelk.search2.parse.Parse, but modified for the pure CQL case.
 */


import whelk.exception.InvalidQueryException;
import whelk.sru.cql.Lex;

import java.util.*;

public class Parse {

    /**
     * CQL BNF:
     *
     * cqlQuery 	::= 	prefixAssignment cqlQuery | scopedClause
     * prefixAssignment 	::= 	'>' prefix '=' uri | '>' uri
     * scopedClause 	::= 	scopedClause booleanGroup searchClause | searchClause
     * booleanGroup 	::= 	boolean [modifierList]
     * boolean 	::= 	'and' | 'or' | 'not' | 'prox'
     * searchClause 	::= 	'(' cqlQuery ')' | index relation searchTerm | searchTerm
     * relation 	::= 	comparitor [modifierList]
     * comparitor 	::= 	comparitorSymbol | namedComparitor
     * comparitorSymbol 	::= 	'=' | '>' | '<' | '>=' | '<=' | '<>' | '=='
     * namedComparitor 	::= 	identifier
     * modifierList 	::= 	modifierList modifier | modifier
     * modifier 	::= 	'/' modifierName [comparitorSymbol modifierValue]
     * prefix, uri, modifierName, modifierValue, searchTerm, index 	::= 	term
     * term 	::= 	identifier | 'and' | 'or' | 'not' | 'prox' | 'sortby'
     * identifier 	::= 	charString1 | charString2
     */

    public record CqlQuery() {
    }


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

                /*
                System.out.println("After reduction, stack and next symbols:\n\tstack:");//\n\t stack: " + stack);
                for (Object o : stack) {
                    System.out.println("\t\t"+o.toString());
                }
                if (!symbols.isEmpty())
                    System.out.println("\t next: " + lookahead + "\n");
                else
                    System.out.println();
                */

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
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.STRING) {
                    stack.pop();
                    stack.push(new CqlQuery());
                    return true;
                }
            }
        }

        return false;
    }
}
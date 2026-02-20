package whelk.search2.parse;

import whelk.exception.InvalidQueryException;

import java.util.*;

public class Parse {
    /**
     * LALR(1) EBNF
     * (ORCOMB is the root node)
     *
     * <p>
     * ORCOMB: ANDCOMB ( "OR" ANDCOMB )*
     * GROUP: "(" ORCOMB | ANDCOMB | GROUP ")" | "(" ")"
     * ANDCOMB: TERM ( "AND" TERM | TERM )*
     * TERM: STRING | GROUP | UOPERATOR TERM | STRING BOPERATOR STRING | STRING BOPERATOREQ TERM
     * UOPERATOR: "NOT"
     * BOPERATOR: "<" | ">" | "<=" | ">="
     * BOPERATOREQ: ":" | "="
     * STRING: ...
     */

    public record Group(OrComb o, AndComb a, Group g) {
    }

    public record OrComb(List<AndComb> andCombs) {
    }

    public record AndComb(List<Term> ts) {
    }

    public record Term(Lex.Symbol string1, Uoperator uop, Term term, Group group, Boperator bop, BoperatorEq bopeq,
                       Lex.Symbol string2) {
    }

    public record Uoperator(Lex.Symbol s, Lex.Symbol c) {
    }

    public record Boperator(Lex.Symbol op) {
    }

    public record BoperatorEq() {
    }

    public static OrComb parseQuery(LinkedList<Lex.Symbol> symbols) throws InvalidQueryException {
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

        if (stack.size() == 1 && stack.getFirst() instanceof OrComb) {
            return (OrComb) stack.getFirst();
        }

        throw new InvalidQueryException("Syntax error");
    }

    // Note to self, the front of the list counts as the top!
    private static void shift(LinkedList<Object> stack, LinkedList<Lex.Symbol> symbols) {
        stack.push(symbols.poll());
    }

    private static boolean reduce(LinkedList<Object> stack, Lex.Symbol lookahead) {

        // BOPERATOR: "<" | ">" | "<=" | ">="
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.OPERATOR &&
                        (s.value().equals("<") || s.value().equals(">") || s.value().equals("<=") || s.value().equals(">="))) {
                    stack.pop();
                    stack.push(new Boperator(s));
                    return true;
                }
            }
        }

        // BOPERATOREQ: ":" | "="
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.OPERATOR &&
                        (s.value().equals("=") || s.value().equals(":"))) {
                    stack.pop();
                    stack.push(new BoperatorEq());
                    return true;
                }
            }
        }

        // UOPERATOR: "NOT"
        {
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        s.name() == Lex.TokenName.KEYWORD &&
                        s.value().equals("not")) {
                    stack.pop();
                    stack.push(new Uoperator(s, null));
                    return true;
                }
            }
        }

        // TERM: STRING | GROUP | UOPERATOR TERM | STRING BOPERATOR STRING | STRING BOPERATOREQ TERM
        {
            if (stack.size() >= 3) {

                // STRING BOPERATOR STRING
                if (stack.get(2) instanceof Lex.Symbol s1 && (s1.name() == Lex.TokenName.STRING || s1.name() == Lex.TokenName.QUOTED_STRING)) {
                    if (stack.get(1) instanceof Boperator bop) {
                        if (stack.getFirst() instanceof Lex.Symbol s3 && (s3.name() == Lex.TokenName.STRING || s3.name() == Lex.TokenName.QUOTED_STRING)) {
                            stack.pop();
                            stack.pop();
                            stack.pop();
                            stack.push(new Term(s3, null, null, null, bop, null, s1));
                            return true;
                        }
                    }
                }

                // STRING BOPERATOREQ TERM
                if (stack.get(2) instanceof Lex.Symbol s1 && (s1.name() == Lex.TokenName.STRING || s1.name() == Lex.TokenName.QUOTED_STRING)) {
                    if (stack.get(1) instanceof BoperatorEq bop) {
                        if (stack.getFirst() instanceof Term t) {
                            stack.pop();
                            stack.pop();
                            stack.pop();
                            stack.push(new Term(null, null, t, null, null, bop, s1));
                            return true;
                        }
                    }
                }
            }
            if (stack.size() >= 2) {
                if (stack.get(1) instanceof Uoperator uop) {
                    if (stack.getFirst() instanceof Term t) {
                        stack.pop();
                        stack.pop();
                        stack.push(new Term(null, uop, t, null, null, null, null));
                        return true;
                    }
                }
            }
            if (!stack.isEmpty()) {
                if (stack.getFirst() instanceof Lex.Symbol s &&
                        (s.name() == Lex.TokenName.STRING || s.name() == Lex.TokenName.QUOTED_STRING)) {

                    boolean okToReduce = true; // Assumption
                    if (lookahead != null) {
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("<"))
                            okToReduce = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals(">"))
                            okToReduce = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("<="))
                            okToReduce = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals(">="))
                            okToReduce = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("="))
                            okToReduce = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals(":"))
                            okToReduce = false;
                    }

                    if (okToReduce) {
                        stack.pop();
                        stack.push(new Term(s, null, null, null, null, null, null));
                        return true;
                    }
                }
                if (stack.getFirst() instanceof Group g) {
                    stack.pop();
                    stack.push(new Term(null, null, null, g, null, null, null));
                    return true;
                }
            }
        }

        // ANDCOMB: TERM ( "AND" TERM | TERM )*
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof Term t) {

                    // We must check that we have everything that goes into the list, *on*
                    // the stack before reducing. In other words, our lookahead must be
                    // something that cannot be part of the list (or EOF) before we reduce.

                    boolean wholeListOnStack = true; // Assumption
                    if (lookahead != null) {
                        if (lookahead.name() == Lex.TokenName.STRING || lookahead.name() == Lex.TokenName.QUOTED_STRING)
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("!"))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("~"))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals(":"))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("="))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.OPERATOR && lookahead.value().equals("("))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.KEYWORD && lookahead.value().equals("not"))
                            wholeListOnStack = false;
                        if (lookahead.name() == Lex.TokenName.KEYWORD && lookahead.value().equals("and"))
                            wholeListOnStack = false;
                    }

                    if (wholeListOnStack) {
                        List<Term> terms = new ArrayList<>();
                        terms.addFirst(t);
                        stack.pop();

                        // Chew the whole list all at once
                        boolean stillChewing;
                        do {
                            stillChewing = false;
                            if (!stack.isEmpty() && stack.get(0) instanceof Term nextTerm) {
                                stack.pop();
                                terms.addFirst(nextTerm);
                                stillChewing = true;
                            } else if (stack.size() >= 2 && stack.get(0) instanceof Lex.Symbol s &&
                                    s.name() == Lex.TokenName.KEYWORD &&
                                    s.value().equals("and") &&
                                    stack.get(1) instanceof Term nextTerm) {
                                stack.pop();
                                stack.pop();
                                terms.addFirst(nextTerm);
                                stillChewing = true;
                            }
                        } while (stillChewing);

                        stack.push(new AndComb(terms));
                        return true;
                    }
                }
            }
        }

        // ORCOMB: ANDCOMB ( "OR" ANDCOMB )*
        {
            if (!stack.isEmpty()) {
                if (stack.get(0) instanceof AndComb ac) {

                    boolean wholeListOnStack = true; // Assumption
                    if (lookahead != null) {
                        if (lookahead.name() == Lex.TokenName.KEYWORD && lookahead.value().equals("or"))
                            wholeListOnStack = false;
                    }

                    if (wholeListOnStack) {
                        List<AndComb> ACs = new ArrayList<>();
                        ACs.addFirst(ac);
                        stack.pop();

                        // Chew the whole list all at once
                        boolean stillChewing;
                        do {
                            stillChewing = false;
                            if (stack.size() >= 2 && stack.get(0) instanceof Lex.Symbol s &&
                                    s.name() == Lex.TokenName.KEYWORD &&
                                    s.value().equals("or") &&
                                    stack.get(1) instanceof AndComb nextAc) {
                                stack.pop();
                                stack.pop();
                                ACs.addFirst(nextAc);
                                stillChewing = true;
                            }
                        } while (stillChewing);

                        stack.push(new OrComb(ACs));
                        return true;
                    }
                }
            }
        }

        // GROUP: "(" ORCOMB | ANDCOMB | GROUP ")" | "(" ")"
        {
            if (stack.size() >= 3) {
                if (stack.get(0) instanceof Lex.Symbol s1 &&
                        s1.name() == Lex.TokenName.OPERATOR &&
                        s1.value().equals(")") &&
                        stack.get(2) instanceof Lex.Symbol s2 &&
                        s2.name() == Lex.TokenName.OPERATOR &&
                        s2.value().equals("(")) {

                    if (stack.get(1) instanceof OrComb oc) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new Group(oc, null, null));
                        return true;
                    }
                    if (stack.get(1) instanceof AndComb ac) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new Group(null, ac, null));
                        return true;
                    }
                    if (stack.get(1) instanceof Group g) {
                        stack.pop();
                        stack.pop();
                        stack.pop();
                        stack.push(new Group(null, null, g));
                        return true;
                    }

                }
            }
            if (stack.size() >= 2) {
                if (stack.get(0) instanceof Lex.Symbol s1 &&
                        s1.name() == Lex.TokenName.OPERATOR &&
                        s1.value().equals(")") &&
                        stack.get(1) instanceof Lex.Symbol s2 &&
                        s2.name() == Lex.TokenName.OPERATOR &&
                        s2.value().equals("(")) {
                    stack.pop();
                    stack.pop();
                    stack.push(new Group(null, new AndComb(List.of()), null));
                    return true;
                }
            }
        }

        return false;
    }
}
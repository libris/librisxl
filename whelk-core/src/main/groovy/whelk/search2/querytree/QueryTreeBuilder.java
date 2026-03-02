package whelk.search2.querytree;

import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.Query;
import whelk.search2.parse.Ast;
import whelk.search2.parse.Lex;
import whelk.search2.parse.Parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        if (queryString.isEmpty()) {
            return new Any.EmptyString();
        } else if (queryString.equals(Operator.WILDCARD)) {
            return new Any.Wildcard();
        }
        return buildTree(getAst(queryString).tree, disambiguate, null, null, queryString);
    }

    private static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, selector, operator, q);
            case Ast.Not n -> buildFromNot(n, disambiguate, selector, operator, q);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, selector, operator);
            case Ast.Code c -> buildFromCode(c, disambiguate, selector, q);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        if (group.operands().isEmpty()) {
            return selector != null
                    ? new Condition(selector, operator, new Any.EmptyGroup())
                    : new Any.EmptyGroup();
        }

        List<Node> children = new ArrayList<>();
        List<Token> freeTextTokens = new ArrayList<>();
        int freeTextStartIdx = -1;

        for (int i = 0; i < group.operands().size(); i++) {
            Ast.Node o = group.operands().get(i);
            if (o instanceof Ast.Leaf leaf) {
                Node node = buildFromLeaf(leaf, disambiguate, selector, operator);
                switch (node) {
                    case FreeText ft -> freeTextTokens.add(ft.tokens().getFirst());
                    case Condition c when c.value() instanceof FreeText ft -> freeTextTokens.add(ft.tokens().getFirst());
                    default -> children.add(node);
                }
            }
            /*
             * Normally, only Ast.Leaf nodes produce FreeText. However, Ast.Code may also
             * yield FreeText when the key is invalid, so we must handle those cases here
             * as well when merging free-text tokens.
             */
            else if (o instanceof Ast.Code c) {
                Node node = buildFromCode(c, disambiguate, selector, q);
                if (node instanceof FreeText ft) {
                    freeTextTokens.add(ft.tokens().getFirst());
                } else {
                    children.add(node);
                }
            } else {
                children.add(buildTree(o, disambiguate, selector, operator, q));
            }
            if (!freeTextTokens.isEmpty() && freeTextStartIdx == -1) {
                freeTextStartIdx = i;
            }
        }

        if (!freeTextTokens.isEmpty()) {
            Query.Connective connective = switch (group) {
                case Ast.And ignored -> Query.Connective.AND;
                case Ast.Or ignored -> Query.Connective.OR;
            };

            FreeText freeText = new FreeText(disambiguate.getTextQueryProperty(), freeTextTokens, connective);
            Node node = selector != null ? new Condition(selector, operator, freeText) : freeText;

            if (children.isEmpty()) {
                return node;
            }

            children.add(freeTextStartIdx, node);
        }

        return switch (group) {
            case Ast.And ignored -> new And(children);
            case Ast.Or ignored -> new Or(children);
        };
    }

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, selector, operator, q).getInverse();
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
        if (selector != null) {
            return buildCondition(selector, operator, leaf, disambiguate);
        }

        Lex.Symbol symbol = leaf.value();

        Optional<FilterAlias> filterAlias = disambiguate.mapToFilter(symbol.value());
        if (filterAlias.isPresent()) {
            var af = filterAlias.get();
            af.parse(disambiguate);
            return af;
        }

        return new FreeText(disambiguate.getTextQueryProperty(), getToken(symbol));
    }

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate, Selector selector, String q) throws InvalidQueryException {
        if (selector != null) {
            // Nested selectors are not allowed, return the inner code segment as free text
            return new Condition(selector, c.operator(), asFreeText(c, q, disambiguate.getTextQueryProperty()));
        }
        selector = disambiguate.mapQueryKey(getToken(c.code()));
        return selector.isValid()
                ? buildTree(c.operand(), disambiguate, selector, c.operator(), q)
                : asFreeText(c, q, disambiguate.getTextQueryProperty()); // If the selector isn't valid, treat the whole segment as free text.
    }

    private static Condition buildCondition(Selector selector, Operator operator, Ast.Leaf leaf, Disambiguate disambiguate) {
        Token token = getToken(leaf.value());
        if (disambiguate.isRestrictedByValue(selector)) {
            selector = disambiguate.restrictByValue(selector, token.value());
        }
        Value value = disambiguate.mapValueForSelector(selector, token).orElse(new FreeText(token));
        Condition condition = new Condition(selector, operator, value);
        return condition.isTypeNode() ? condition.asTypeNode() : condition;
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }

    private static FreeText asFreeText(Ast.Code c, String q, Property.TextQuery textQuery) {
        int from = c.code().offset();
        int to = findSegmentEndIdx(c, q);
        String s = q.substring(from, to);
        return new FreeText(textQuery, new Token.Raw(s, from));
    }

    private static int findSegmentEndIdx(Ast.Code c, String q) {
        Rightmost rightmost = findRightmost(c, 0, q);
        var depth = rightmost.parenDepth();
        var rightmostSymbol = rightmost.symbol() != null ? rightmost.symbol() : c.code();
        var rightmostEnd = endIdx(rightmostSymbol);
        if (depth > 0) {
            int closing = findNthClosingParenthesis(q.substring(rightmostEnd), depth);
            if (closing == -1) {
                // Unreachable
                throw new IllegalStateException("Unbalanced parentheses after AST parsing");
            }
            return rightmostEnd + closing + 1;
        }
        return rightmostEnd;
    }

    private static int endIdx(Lex.Symbol symbol) {
        return symbol.offset() + symbol.value().length() + (symbol.isQuoted() ? 2 : 0);
    }

    private record Rightmost(Lex.Symbol symbol, int parenDepth) {}

    private static Rightmost findRightmost(Ast.Node n, int parenDepth, String q) {
        return switch (n) {
            case Ast.Group g -> {
                int nextDepth = parenDepth + 1;
                yield g.operands().isEmpty()
                        ? new Rightmost(null, nextDepth)
                        : findRightmost(g.operands().getLast(), nextDepth, q);
            }
            case Ast.Code c -> {
                int nextDepth = parenDepth + (isAtomicWrappedInParentheses(c.operand(), q) ? 1 : 0);
                yield findRightmost(c.operand(), nextDepth, q);
            }
            case Ast.Leaf l -> new Rightmost(l.value(), parenDepth);
            case Ast.Not not -> findRightmost(not.operand(), parenDepth, q);
        };
    }

    private static boolean isAtomicWrappedInParentheses(Ast.Node operand, String q) {
        return switch (operand) {
            // We only need to check for an opening parenthesis since unbalanced parentheses won't pass AST parsing
            case Ast.Leaf l -> isPrecededByOpeningParen(l.value(), q);
            case Ast.Code c -> isPrecededByOpeningParen(c.code(), q);
            default -> false;
        };
    }

    private static boolean isPrecededByOpeningParen(Lex.Symbol symbol, String q) {
        int start = symbol.offset();

        for (int i = start - 1; i >= 0; i--) {
            var c = q.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            return c == '(';
        }

        return false;
    }

    private static int findNthClosingParenthesis(String s, int n) {
        int count = 0;

        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ')') {
                if (++count == n) {
                    return i;
                }
            }
        }

        return -1;
    }
}

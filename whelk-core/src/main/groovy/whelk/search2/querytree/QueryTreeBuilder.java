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
import java.util.function.Predicate;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        if (queryString.isEmpty()) {
            return new Any.EmptyString();
        } else if (queryString.equals(Operator.WILDCARD)) {
            return new Any.Wildcard();
        }
        return buildTree(getAst(queryString).tree, disambiguate, null, null, queryString);
    }

    private static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, MappedCode mc, Operator operator, String q) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, mc, operator, q);
            case Ast.Not n -> buildFromNot(n, disambiguate, mc, operator, q);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, mc, operator);
            case Ast.Code c -> mc != null
                    ? asFreeText(mc.astCode(), q, disambiguate.getTextQueryProperty()) // Codes within code groups are not allowed, treat the whole code segment as free text
                    : buildFromCode(c, disambiguate, q);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, MappedCode mc, Operator operator, String q) throws InvalidQueryException {
        if (group.operands().isEmpty()) {
            return mc != null
                    ? new Condition(mc.selector(), operator, new Any.EmptyGroup())
                    : new Any.EmptyGroup();
        }

        List<Node> children = new ArrayList<>();
        List<Token> freeTextTokens = new ArrayList<>();
        int freeTextStartIdx = -1;

        for (int i = 0; i < group.operands().size(); i++) {
            Ast.Node o = group.operands().get(i);
            if (o instanceof Ast.Leaf leaf) {
                Node node = buildFromLeaf(leaf, disambiguate, mc, operator);
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
                if (mc != null) {
                    // Codes within code groups are not allowed, return the whole code group as free text
                    return asFreeText(mc.astCode(), q, disambiguate.getTextQueryProperty());
                }
                Node node = buildFromCode(c, disambiguate, q);
                if (node instanceof FreeText ft) {
                    freeTextTokens.add(ft.tokens().getFirst());
                } else {
                    children.add(node);
                }
            } else {
                children.add(buildTree(o, disambiguate, mc, operator, q));
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
            Node node = mc != null ? new Condition(mc.selector(), operator, freeText) : freeText;

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

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, MappedCode mc, Operator operator, String q) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, mc, operator, q).getInverse();
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, MappedCode mc, Operator operator) throws InvalidQueryException {
        if (mc != null) {
            return buildCondition(mc.selector(), operator, leaf, disambiguate);
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

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate, String q) throws InvalidQueryException {
        MappedCode mc = MappedCode.from(c, disambiguate);
        return mc.selector().isValid()
                ? buildTree(c.operand(), disambiguate, mc, c.operator(), q)
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

    private record MappedCode(Ast.Code astCode, Selector selector) {
        static MappedCode from(Ast.Code c, Disambiguate disambiguate) {
            return new MappedCode(c, disambiguate.mapQueryKey(getToken(c.code())));
        }
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }

    private static FreeText asFreeText(Ast.Code c, String q, Property.TextQuery textQuery) {
        int from = c.code().offset();
        EndPosition rightMostTokenEnd = findRightMostTokenEnd(c, -1, 0);
        int to = findSegmentEnd(q, rightMostTokenEnd);
        String s = q.substring(from, to);
        return new FreeText(textQuery, new Token.Raw(s, from));
    }

    private record EndPosition(int idx, int nestedLevel) {}

    private static EndPosition findRightMostTokenEnd(Ast.Node n, int currentRightMost, int currentLevel) {
        return switch (n) {
            case Ast.Group g -> g.operands().isEmpty()
                    ? new EndPosition(currentRightMost, currentLevel + 1)
                    : findRightMostTokenEnd(g.operands().getLast(), currentRightMost, currentLevel + 1);
            case Ast.Code c -> findRightMostTokenEnd(c.operand(), c.code().offset() + c.code().value().length(), currentLevel);
            case Ast.Leaf l -> new EndPosition(l.value().offset() + l.value().value().length(), currentLevel);
            case Ast.Not not -> findRightMostTokenEnd(not.operand(), currentRightMost, currentLevel);
        };
    }

    private static int findSegmentEnd(String q, EndPosition rightMostTokenEnd) {
        var level = rightMostTokenEnd.nestedLevel();
        var endIdx = rightMostTokenEnd.idx();
        if (level > 0) {
            var closing = findNthCharOccurrence(q.substring(endIdx), level, c -> c == ')');
            return closing == -1 ? q.length() : endIdx + closing + 1;
        }
        var nextWhitespace = findNthCharOccurrence(q.substring(endIdx), 1, Character::isWhitespace);
        return nextWhitespace == -1 ? q.length() : endIdx + nextWhitespace;
    }

    private static int findNthCharOccurrence(String s, int n, Predicate<Character> charTest) {
        int count = 0;

        for (int i = 0; i < s.length(); i++) {
            if (charTest.test(s.charAt(i))) {
                if (++count == n) {
                    return i;
                }
            }
        }

        return -1;
    }
}

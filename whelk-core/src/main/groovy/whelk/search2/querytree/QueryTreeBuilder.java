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
        return buildTree(getAst(queryString).tree, disambiguate, null, null);
    }

    public static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, selector, operator);
            case Ast.Not n -> buildFromNot(n, disambiguate, selector, operator);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, selector, operator);
            case Ast.Code c -> {
                if (selector != null) {
                    throw new InvalidQueryException("Codes within code groups are not allowed.");
                }
                yield buildFromCode(c, disambiguate);
            }
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
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
                if (!freeTextTokens.isEmpty() && freeTextStartIdx == -1) {
                    freeTextStartIdx = i;
                }
            } else {
                children.add(buildTree(o, disambiguate, selector, operator));
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

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, selector, operator).getInverse();
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
        if (selector != null) {
            return buildStatement(selector, operator, leaf, disambiguate);
        }

        Lex.Symbol symbol = leaf.value();

        Optional<FilterAlias> filterAlias = disambiguate.mapToFilter(symbol.value());
        if (filterAlias.isPresent()) {
            var af = filterAlias.get();
            af.parse(disambiguate);
            return af;
        }

        return new FreeText(disambiguate.getTextQueryProperty(), getToken(leaf.value()));
    }

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate) throws InvalidQueryException {
        Selector selector = disambiguate.mapQueryKey(getToken(c.code()));
        return buildTree(c.operand(), disambiguate, selector, c.operator());
    }

    private static Condition buildStatement(Selector selector, Operator operator, Ast.Leaf leaf, Disambiguate disambiguate) {
        Token token = getToken(leaf.value());
        if (disambiguate.isRestrictedByValue(selector)) {
            selector = disambiguate.restrictByValue(selector, token.value());
        }
        Value value = disambiguate.mapValueForSelector(selector, token).orElse(new FreeText(token));
        Condition statement = new Condition(selector, operator, value);
        return statement.isTypeNode() ? statement.asTypeNode() : statement;
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }
}

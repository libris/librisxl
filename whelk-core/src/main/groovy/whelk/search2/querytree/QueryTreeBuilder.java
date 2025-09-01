package whelk.search2.querytree;

import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.Filter;
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
        return buildTree(getAst(queryString).tree, disambiguate, false, null, null);
    }

    public static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, boolean negate, Path path, Operator operator) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, negate, path, operator);
            case Ast.Not n -> buildFromNot(n, disambiguate, negate, path, operator);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, negate, path, operator);
            case Ast.Code c -> {
                if (path != null) {
                    throw new InvalidQueryException("Codes within code groups are not allowed.");
                }
                yield buildFromCode(c, disambiguate, negate);
            }
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, boolean negate, Path path, Operator operator) throws InvalidQueryException {
        List<Node> children = new ArrayList<>();
        List<Token> freeTextTokens = new ArrayList<>();
        int freeTextStartIdx = -1;

        for (int i = 0; i < group.operands().size(); i++) {
            Ast.Node o = group.operands().get(i);
            if (o instanceof Ast.Leaf leaf) {
                Node node = buildFromLeaf(leaf, disambiguate, negate, path, operator);
                if (node instanceof FreeText ft) {
                    freeTextTokens.add(ft.tokens().getFirst());
                } else if (node instanceof PathValue pv && pv.value() instanceof FreeText ft) {
                    freeTextTokens.add(ft.tokens().getFirst());
                } else {
                    children.add(node);
                }
                if (!freeTextTokens.isEmpty() && freeTextStartIdx == -1) {
                    freeTextStartIdx = i;
                }
            } else {
                children.add(buildTree(o, disambiguate, negate, path, operator));
            }
        }

        if (!freeTextTokens.isEmpty()) {
            Query.Connective connective = switch (group) {
                case Ast.And ignored -> Query.Connective.AND;
                case Ast.Or ignored -> Query.Connective.OR;
            };

            Node node;
            if (path == null) {
                node = new FreeText(disambiguate.getTextQueryProperty(), negate, freeTextTokens, connective);
            } else {
                FreeText freeText = new FreeText(null, false, freeTextTokens, connective);
                node = new PathValue(path, negate ? operator.getInverse() : operator, freeText);
            }

            if (children.isEmpty()) {
                return node;
            }

            children.add(freeTextStartIdx, node);
        }

        return switch (group) {
            case Ast.And ignored -> negate ? new Or(children) : new And(children);
            case Ast.Or ignored -> negate ? new And(children) : new Or(children);
        };
    }

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, boolean negate, Path path, Operator operator) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, !negate, path, operator);
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, boolean negate, Path path, Operator operator) throws InvalidQueryException {
        if (path != null) {
            return buildPathValue(path, operator, leaf, negate, disambiguate);
        }
        Lex.Symbol symbol = leaf.value();
        Optional<Filter.AliasedFilter> filter = disambiguate.mapToFilter(symbol.value());
        if (filter.isPresent()) {
            ActiveFilter activeFilter = new ActiveFilter(filter.get().parseAndGet(disambiguate));
            return negate ? activeFilter.getInverse() : activeFilter;
        }
        return new FreeText(disambiguate.getTextQueryProperty(), negate, getToken(leaf.value()));
    }

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate, boolean negate) throws InvalidQueryException {
        List<Subpath> subpaths = new ArrayList<>();
        int currentOffset = c.code().offset();
        for (String key : c.code().value().split("\\.")) {
            Subpath subpath = disambiguate.mapKey(key, currentOffset);
            subpaths.add(subpath);
            currentOffset += key.length() + 1;
        }

        Path path = new Path(subpaths, getToken(c.code()));

        return buildTree(c.operand(), disambiguate, negate, path, c.operator());
    }

    private static PathValue buildPathValue(Path path, Operator operator, Ast.Leaf leaf, boolean negate, Disambiguate disambiguate) {
        Token token = getToken(leaf.value());
        Value value = path.lastProperty()
                .flatMap(p -> disambiguate.mapValueForProperty(p, token))
                .orElse(new FreeText(token));
        return new PathValue(path, negate ? operator.getInverse() : operator, value);
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }
}

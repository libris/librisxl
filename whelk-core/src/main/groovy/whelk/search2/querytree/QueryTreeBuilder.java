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

    public static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, Path path, Operator operator) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, path, operator);
            case Ast.Not n -> buildFromNot(n, disambiguate, path, operator);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, path, operator);
            case Ast.Code c -> {
                if (path != null) {
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

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, Path path, Operator operator) throws InvalidQueryException {
        List<Node> children = new ArrayList<>();
        List<Token> freeTextTokens = new ArrayList<>();
        int freeTextStartIdx = -1;

        for (int i = 0; i < group.operands().size(); i++) {
            Ast.Node o = group.operands().get(i);
            if (o instanceof Ast.Leaf leaf) {
                Node node = buildFromLeaf(leaf, disambiguate, path, operator);
                switch (node) {
                    case FreeText ft -> freeTextTokens.add(ft.tokens().getFirst());
                    case PathValue pv when pv.value() instanceof FreeText ft -> freeTextTokens.add(ft.tokens().getFirst());
                    default -> children.add(node);
                }
                if (!freeTextTokens.isEmpty() && freeTextStartIdx == -1) {
                    freeTextStartIdx = i;
                }
            } else {
                children.add(buildTree(o, disambiguate, path, operator));
            }
        }

        if (!freeTextTokens.isEmpty()) {
            Query.Connective connective = switch (group) {
                case Ast.And ignored -> Query.Connective.AND;
                case Ast.Or ignored -> Query.Connective.OR;
            };

            FreeText freeText = new FreeText(disambiguate.getTextQueryProperty(), freeTextTokens, connective);
            Node node = path != null ? new PathValue(path, operator, freeText) : freeText;

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

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, Path path, Operator operator) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, path, operator).getInverse();
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, Path path, Operator operator) throws InvalidQueryException {
        if (path != null) {
            return buildPathValue(path, operator, leaf, disambiguate);
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
        List<Subpath> subpaths = new ArrayList<>();
        int currentOffset = c.code().offset();
        for (String key : c.code().value().split("\\.")) {
            Subpath subpath = disambiguate.mapQueryKey(key, currentOffset);
            subpaths.add(subpath);
            currentOffset += key.length() + 1;
        }

        Path path = new Path(subpaths, getToken(c.code()));

        return buildTree(c.operand(), disambiguate, path, c.operator());
    }

    private static PathValue buildPathValue(Path path, Operator operator, Ast.Leaf leaf, Disambiguate disambiguate) {
        if (path.last() instanceof Property p && disambiguate.isRestrictedByValue(p.name())) {
            var narrowed = disambiguate.restrictByValue(p, leaf.value().value());
            var newPath = new ArrayList<>(path.path());
            newPath.removeLast();
            newPath.add(narrowed);
            path = new Path(newPath);
        }

        Token token = getToken(leaf.value());
        Value value = path.lastProperty()
                .flatMap(p -> disambiguate.mapValueForProperty(p, token))
                .orElse(new FreeText(token));

        PathValue pathValue = new PathValue(path, operator, value);
        return pathValue.isTypeNode() ? pathValue.asTypeNode() : pathValue;
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }
}

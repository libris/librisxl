package whelk.search2.querytree;

import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.Filter;
import whelk.search2.Operator;
import whelk.search2.parse.Ast;
import whelk.search2.parse.Lex;
import whelk.search2.parse.Parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        return buildTree(getAst(queryString).tree, disambiguate);
    }

    public static Node buildTree(Ast.Node ast, Disambiguate disambiguate) throws InvalidQueryException {
        return switch (ast) {
            case Ast.And and -> buildFromAnd(and, disambiguate);
            case Ast.Or or -> buildFromOr(or, disambiguate);
            case Ast.Not not -> buildFromNot(not, disambiguate);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate);
            case Ast.Code c -> buildFromCode(c, disambiguate);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromAnd(Ast.And and, Disambiguate disambiguate) throws InvalidQueryException {
        List<Ast.Leaf> freeTextLeaves = new ArrayList<>();
        List<Node> conjuncts = new ArrayList<>();

        for (Ast.Node o : and.operands()) {
            if (isFreeText(o, disambiguate)) {
                freeTextLeaves.add((Ast.Leaf) o);
            } else {
                conjuncts.add(buildTree(o, disambiguate));
            }
        }

        if (freeTextLeaves.isEmpty()) {
            return new And(conjuncts);
        }

        List<Token> tokens = freeTextLeaves.stream()
                .map(Ast.Leaf::value)
                .map(QueryTreeBuilder::getToken)
                .toList();

        FreeText freeText = new FreeText(disambiguate.getTextQueryProperty(), Operator.EQUALS, tokens);

        if (conjuncts.isEmpty()) {
            return freeText;
        }

        conjuncts.add(and.operands().indexOf(freeTextLeaves.getFirst()), freeText);

        return new And(conjuncts);
    }

    private static Node buildFromOr(Ast.Or or, Disambiguate disambiguate) throws InvalidQueryException {
        List<Node> disjuncts = new ArrayList<>();
        for (Ast.Node o : or.operands()) {
            disjuncts.add(buildTree(o, disambiguate));
        }
        return new Or(disjuncts);
    }

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate) throws InvalidQueryException {
        Ast.Leaf leaf = (Ast.Leaf) not.operand();
        Optional<Filter.AliasedFilter> filter = disambiguate.mapToFilter(getValue(leaf));
        if (filter.isPresent()) {
            return new ActiveFilter(filter.get().parseAndGet(disambiguate)).getInverse();
        }
        return new FreeText(disambiguate.getTextQueryProperty(), Operator.NOT_EQUALS, getToken(leaf.value()));
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate) throws InvalidQueryException {
        Optional<Filter.AliasedFilter> filter = disambiguate.mapToFilter(getValue(leaf));
        if (filter.isPresent()) {
            return new ActiveFilter(filter.get().parseAndGet(disambiguate));
        }
        return new FreeText(disambiguate.getTextQueryProperty(), Operator.EQUALS, getToken(leaf.value()));
    }

    private static PathValue buildFromCode(Ast.Code c, Disambiguate disambiguate) {
        List<Subpath> subpaths = new ArrayList<>();
        int currentOffset = c.code().offset();
        for (String key : c.code().value().split("\\.")) {
            Subpath subpath = disambiguate.mapKey(key, currentOffset);
            subpaths.add(subpath);
            currentOffset += key.length() + 1;
        }

        Path path = new Path(subpaths, getToken(c.code()));

        Token valueToken = getToken(((Ast.Leaf) c.operand()).value());

        Value value = path.lastProperty()
                .map(p -> disambiguate.getValueForProperty(p, valueToken))
                .orElse(new Literal(valueToken));

        return new PathValue(path, c.operator(), value);
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset())
                : new Token.Raw(symbol.value(), symbol.offset());
    }

    private static String getValue(Ast.Leaf leaf) {
        return leaf.value().value();
    }

    private static boolean isFreeText(Ast.Node astNode, Disambiguate disambiguate) {
        return astNode instanceof Ast.Leaf(Lex.Symbol symbol) && disambiguate.mapToFilter(symbol.value()).isEmpty();
    }
}

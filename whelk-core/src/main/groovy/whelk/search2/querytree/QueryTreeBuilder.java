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
import java.util.stream.Stream;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        return buildTree(getAst(queryString).tree, disambiguate);
    }

    public static Node buildTree(Ast.Node ast, Disambiguate disambiguate) throws InvalidQueryException {
        return switch (ast) {
            case Ast.And and -> buildAnd(and, disambiguate);
            case Ast.Or or -> buildOr(or, disambiguate);
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

    private static Node buildAnd(Ast.And and, Disambiguate disambiguate) throws InvalidQueryException {
        List<Node> conjuncts = new ArrayList<>();
        for (Ast.Node o : and.operands()) {
            conjuncts.add(buildTree(o, disambiguate));
        }
        return new And(conjuncts);
    }

    private static Node buildOr(Ast.Or or, Disambiguate disambiguate) throws InvalidQueryException {
        List<Node> disjuncts = new ArrayList<>();
        for (Ast.Node o : or.operands()) {
            disjuncts.add(buildTree(o, disambiguate));
        }
        return new Or(disjuncts);
    }

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate) throws InvalidQueryException {
        String value = ((Ast.Leaf) not.operand()).value();
        Optional<Filter.AliasedFilter> filter = disambiguate.mapToFilter(value);
        return filter.isPresent()
                ? new InactiveFilter(filter.get().parseAndGet(disambiguate))
                : new FreeText(disambiguate.getTextQueryProperty(), Operator.NOT_EQUALS, value);
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate) throws InvalidQueryException {
        String value = leaf.value();
        Optional<Filter.AliasedFilter> filter = disambiguate.mapToFilter(value);
        return filter.isPresent()
                ? new ActiveFilter(filter.get().parseAndGet(disambiguate))
                : new FreeText(disambiguate.getTextQueryProperty(), Operator.EQUALS, leaf.value());
    }

    private static PathValue buildFromCode(Ast.Code c, Disambiguate disambiguate) {
        Path path = new Path(Stream.of(c.code().split("\\."))
                .map(disambiguate::mapKey)
                .toList());

        String rawValue = ((Ast.Leaf) c.operand()).value();
        Value value = path.lastProperty()
                .map(p -> disambiguate.getValueForProperty(p, rawValue))
                .orElse(new Literal(rawValue));

        return new PathValue(path, c.operator(), value);
    }
}

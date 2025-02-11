package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.parse.Ast;
import whelk.search2.parse.Lex;
import whelk.search2.parse.Parse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static whelk.JsonLd.LD_KEYS;
import static whelk.JsonLd.looksLikeIri;
import static whelk.search2.Disambiguate.expandPrefixed;
import static whelk.search2.QueryUtil.encodeUri;
import static whelk.search2.QueryUtil.loadThing;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate, Whelk whelk, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        return buildTree(getAst(queryString).tree, disambiguate, whelk, aliasToFilter);
    }

    private static Node buildTree(Ast.Node ast, Disambiguate disambiguate, Whelk whelk, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        return switch (ast) {
            case Ast.And and -> buildAnd(and, disambiguate, whelk, aliasToFilter);
            case Ast.Or or -> buildOr(or, disambiguate, whelk, aliasToFilter);
            case Ast.Not not -> buildFromNot(not, whelk.getJsonld(), aliasToFilter);
            case Ast.Leaf l -> buildFromLeaf(l, whelk.getJsonld(), aliasToFilter);
            case Ast.Code c -> buildFromCode(c, disambiguate, whelk);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildAnd(Ast.And and, Disambiguate disambiguate, Whelk whelk, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> conjuncts = new ArrayList<>();
        for (Ast.Node o : and.operands()) {
            conjuncts.add(buildTree(o, disambiguate, whelk, aliasToFilter));
        }
        return new And(conjuncts);
    }

    private static Node buildOr(Ast.Or or, Disambiguate disambiguate, Whelk whelk, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> disjuncts = new ArrayList<>();
        for (Ast.Node o : or.operands()) {
            disjuncts.add(buildTree(o, disambiguate, whelk, aliasToFilter));
        }
        return new Or(disjuncts);
    }

    private static Node buildFromNot(Ast.Not not, JsonLd jsonLd, Map<String, AppParams.Filter> aliasToFilter) {
        String value = ((Ast.Leaf) not.operand()).value();
        return aliasToFilter.containsKey(value)
                ? new InactiveBoolFilter(value)
                : new FreeText(Operator.NOT_EQUALS, value, jsonLd);
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, JsonLd jsonLd, Map<String, AppParams.Filter> aliasToFilter) {
        var filter = aliasToFilter.get(leaf.value());
        if (filter == null) {
            return new FreeText(Operator.EQUALS, leaf.value(), jsonLd);
        }
        return new ActiveBoolFilter(leaf.value(), filter.getExplicit(), filter.getPrefLabelByLang());
    }

    private static PathValue buildFromCode(Ast.Code c, Disambiguate disambiguate, Whelk whelk) {
        List<Subpath> path = new ArrayList<>();

        for (String part : c.code().split("\\.")) {
            // TODO: Look up all indexed keys starting with underscore?
            if (LD_KEYS.contains(part) || part.startsWith("_")) {
                path.add(new Key.RecognizedKey(part));
            } else {
                Optional<String> mappedProperty = disambiguate.mapToProperty(part);
                if (mappedProperty.isPresent()) {
                    path.add(new Property(mappedProperty.get(), whelk.getJsonld(), part));
                } else {
                    path.add(disambiguate.getAmbiguousPropertyMapping(part).isEmpty()
                            ? new Key.UnrecognizedKey(part)
                            : new Key.AmbiguousKey(part));
                }
            }
        }

        String value = ((Ast.Leaf) c.operand()).value();
        Path pathObj = new Path(path);

        if (!pathObj.isValid()) {
            return new PathValue(new Path.ExpandedPath(path), c.operator(), new Literal(value));
        }

        Value valueObj = pathObj.lastProperty().isPresent()
                ? buildValue(pathObj.lastProperty().get(), value, disambiguate, whelk)
                : new Literal(value);

        return new PathValue(pathObj, c.operator(), valueObj);
    }

    private static Value buildValue(Property property, String value, Disambiguate disambiguate, Whelk whelk) {
        if (value.equals(Operator.WILDCARD)) {
            return new Literal(value);
        }
        if (property.isType()) {
            Optional<String> mappedType = disambiguate.mapToKbvClass(value);
            if (mappedType.isPresent()) {
                return new VocabTerm(mappedType.get(), whelk.getJsonld().vocabIndex.get(mappedType.get()), value);
            } else {
                return disambiguate.getAmbiguousClassMapping(value).isEmpty()
                        ? new InvalidValue.ForbiddenValue(value)
                        : new InvalidValue.AmbiguousValue(value);
            }
        } else if (whelk.getJsonld().isVocabTerm(property.name())) {
            Optional<String> mappedEnum = disambiguate.mapToEnum(value);
            if (mappedEnum.isPresent()) {
                return new VocabTerm(mappedEnum.get(), whelk.getJsonld().vocabIndex.get(mappedEnum.get()), value);
            } else {
                return disambiguate.getAmbiguousEnumMapping(value).isEmpty()
                        ? new InvalidValue.ForbiddenValue(value)
                        : new InvalidValue.AmbiguousValue(value);
            }
        }
        // Expand and encode URIs, e.g. sao:Hästar -> https://id.kb.se/term/sao/H%C3%A4star
        else if (property.isObjectProperty()) {
            String expanded = expandPrefixed(value);
            if (looksLikeIri(expanded)) {
                var encoded = encodeUri(expanded);
                return new Link(encoded, whelk.getJsonld().toChip(loadThing(encoded, whelk)), value);
            } else {
                return new Literal(value);
            }
        } else {
            return new Literal(value);
        }
    }
}

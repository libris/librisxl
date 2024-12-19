package whelk.search2.querytree;

import whelk.JsonLd;
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

import static whelk.search2.QueryUtil.encodeUri;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        return buildTree(getAst(queryString).tree, disambiguate, aliasToFilter);
    }

    private static Node buildTree(Ast.Node ast, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        return switch (ast) {
            case Ast.And and -> buildAnd(and, disambiguate, aliasToFilter);
            case Ast.Or or -> buildOr(or, disambiguate, aliasToFilter);
            case Ast.Not not -> buildFromNot(not, aliasToFilter);
            case Ast.Leaf l -> buildFromLeaf(l, aliasToFilter);
            case Ast.Code c -> buildFromCode(c, disambiguate);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildAnd(Ast.And and, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> conjuncts = new ArrayList<>();
        for (Ast.Node o : and.operands()) {
            conjuncts.add(buildTree(o, disambiguate, aliasToFilter));
        }
        return new And(conjuncts);
    }

    private static Node buildOr(Ast.Or or, Disambiguate disambiguate, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        List<Node> disjuncts = new ArrayList<>();
        for (Ast.Node o : or.operands()) {
            disjuncts.add(buildTree(o, disambiguate, aliasToFilter));
        }
        return new Or(disjuncts);
    }

    private static Node buildFromNot(Ast.Not not, Map<String, AppParams.Filter> aliasToFilter) {
        String value = ((Ast.Leaf) not.operand()).value();
        return aliasToFilter.containsKey(value)
                ? new InactiveBoolFilter(value)
                : new FreeText(Operator.NOT_EQUALS, value);
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Map<String, AppParams.Filter> aliasToFilter) {
        var filter = aliasToFilter.get(leaf.value());
        if (filter == null) {
            return new FreeText(Operator.EQUALS, leaf.value());
        }
        return new ActiveBoolFilter(leaf.value(), filter.getExplicit(), filter.getPrefLabelByLang());
    }

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate) throws InvalidQueryException {
        Optional<String> property = disambiguate.mapToProperty(c.code());
        if (property.isPresent()) {
            Property p = new Property(property.get(), disambiguate);
            Value v = buildValue(p, ((Ast.Leaf) c.operand()).value(), disambiguate);
            return new PropertyValue(p, c.operator(), v);
        }
        return buildPathValue(c, disambiguate);
    }

    private static PathValue buildPathValue(Ast.Code c, Disambiguate disambiguate) throws InvalidQueryException {
        List<Object> path = new ArrayList<>();

        for (String part : c.code().split("\\.")) {
            Optional<String> mappedProperty = disambiguate.mapToProperty(part);
            if (mappedProperty.isPresent()) {
                path.add(new Property(mappedProperty.get(), disambiguate));
            } else if (Disambiguate.isLdKey(part) || JsonLd.SEARCH_KEY.equals(part)) {
                path.add(part);
            } else {
                var ambiguous = disambiguate.getAmbiguousPropertyMapping(part);
                if (ambiguous.isEmpty()) {
                    path.add(new InvalidKey.UnrecognizedKey(part));
                } else {
                    path.add(new InvalidKey.AmbiguousKey(part));
                }
            }
        }

        String value = ((Ast.Leaf) c.operand()).value();
        Path pathObj = new Path(path);
        Value valueObj = pathObj.mainProperty().isEmpty()
                ? new Literal(value)
                : buildValue(pathObj.mainProperty().get(), value, disambiguate);

        return new PathValue(path, c.operator(), valueObj);
    }

    private static Value buildValue(Property property, String value, Disambiguate disambiguate) throws InvalidQueryException {
        if (value.equals(Operator.WILDCARD)) {
            return new Literal(value);
        }
        if (property.isType()) {
            Optional<String> mappedType = disambiguate.mapToKbvClass(value);
            if (mappedType.isPresent()) {
                return new VocabTerm(mappedType.get(), disambiguate.getDefinition(mappedType.get()));
            } else {
                return disambiguate.getAmbiguousClassMapping(value).isEmpty()
                        ? new InvalidValue.ForbiddenValue(value)
                        : new InvalidValue.AmbiguousValue(value);
            }
        } else if (property.isVocabTerm()) {
            Optional<String> mappedEnum = disambiguate.mapToEnum(value);
            if (mappedEnum.isPresent()) {
                return new VocabTerm(mappedEnum.get(), disambiguate.getDefinition(mappedEnum.get()));
            } else {
                return disambiguate.getAmbiguousEnumMapping(value).isEmpty()
                        ? new InvalidValue.ForbiddenValue(value)
                        : new InvalidValue.AmbiguousValue(value);
            }
        }
        // Expand and encode URIs, e.g. sao:Hästar -> https://id.kb.se/term/sao/H%C3%A4star
        else if (property.isObjectProperty()) {
            String expanded = Disambiguate.expandPrefixed(value);
            if (JsonLd.looksLikeIri(expanded)) {
                var encoded = encodeUri(expanded);
                return new Link(encoded, disambiguate.getChip(encoded));
            } else {
                return new Literal(value);
            }
        } else {
            return new Literal(value);
        }
    }
}

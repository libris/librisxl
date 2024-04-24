package whelk.xlql;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static whelk.search.XLQLQuery.encodeUri;
import static whelk.search.XLQLQuery.quoteIfPhraseOrContainsSpecialSymbol;


public class SimpleQueryTree {
    public sealed interface Node permits And, Or, PropertyValue, FreeText {
    }

    public record And(List<Node> conjuncts) implements Node {
    }

    public record Or(List<Node> disjuncts) implements Node {
    }

    public record PropertyValue(String property, List<String> propertyPath, Operator operator,
                                Value value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
    }

    public sealed interface Value permits Link, Literal, VocabTerm {
        String string();
    }

    public record Literal(String string) implements Value {
    }

    public record Link(String string) implements Value {
    }

    public record VocabTerm(String string) implements Value {
    }

    public Node tree;

    private List<PropertyValue> topPvNodes;

    private String freeTextPart;

    public SimpleQueryTree(FlattenedAst ast, Disambiguate disambiguate) throws InvalidQueryException {
        this.tree = buildTree(ast.tree, disambiguate);
    }

    public SimpleQueryTree(Node tree) {
        this.tree = tree;
    }

    private static Node buildTree(FlattenedAst.Node ast, Disambiguate disambiguate) throws InvalidQueryException {
        switch (ast) {
            case FlattenedAst.And and -> {
                List<Node> conjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : and.operands()) {
                    conjuncts.add(buildTree(o, disambiguate));
                }
                return new And(conjuncts);
            }
            case FlattenedAst.Or or -> {
                List<Node> disjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : or.operands()) {
                    disjuncts.add(buildTree(o, disambiguate));
                }
                return new Or(disjuncts);
            }
            case FlattenedAst.Not not -> {
                return new FreeText(Operator.NOT_EQUALS, not.value());
            }
            case FlattenedAst.Leaf l -> {
                return new FreeText(Operator.EQUALS, l.value());
            }
            case FlattenedAst.Code c -> {
                String property = null;
                String value = c.value();
                List<String> propertyPath = new ArrayList<>();

                for (String part : c.code().split("\\.")) {
                    Optional<String> mappedProperty = disambiguate.mapToProperty(part);
                    if (mappedProperty.isPresent()) {
                        property = mappedProperty.get();
                        propertyPath.add(property);
                    } else if (Disambiguate.isLdKey(part) || JsonLd.SEARCH_KEY.equals(part)) {
                        propertyPath.add(part);
                    } else {
                        throw new InvalidQueryException("Unrecognized property alias: " + part);
                    }
                }

                Value v;

                if (disambiguate.isType(property)) {
                    Optional<String> mappedType = disambiguate.mapToKbvClass(value);
                    if (mappedType.isPresent()) {
                        v = new VocabTerm(mappedType.get());
                    } else {
                        throw new InvalidQueryException("Unrecognized type: " + value);
                    }
                } else if (disambiguate.isVocabTerm(property)) {
                    Optional<String> mappedEnum = disambiguate.mapToEnum(value);
                    if (mappedEnum.isPresent()) {
                        v = new VocabTerm(mappedEnum.get());
                    } else {
                        throw new InvalidQueryException("Invalid value " + value + " for property " + property);
                    }
                }
                // Expand and encode URIs, e.g. sao:Hästar -> https://id.kb.se/term/sao/H%C3%A4star
                else if (disambiguate.isObjectProperty(property)) {
                    String expanded = Disambiguate.expandPrefixed(value);
                    v = JsonLd.looksLikeIri(expanded) ? new Link(encodeUri(expanded)) : new Literal(value);
                } else {
                    v = new Literal(value);
                }

                return new PropertyValue(property, propertyPath, c.operator(), v);
            }
        }
    }

    public SimpleQueryTree andExtend(Node node) {
        var conjuncts = switch (tree) {
            case And and -> {
                var copy = new ArrayList<>(and.conjuncts());
                if (!copy.contains(node)) {
                    copy.add(node);
                }
                yield copy;
            }
            default -> tree.equals(node) ? List.of(tree) : List.of(tree, node);
        };

        return new SimpleQueryTree(conjuncts.size() == 1 ? conjuncts.getFirst() : new And(conjuncts));
    }

    public SimpleQueryTree excludeFromTree(Node node) {
        return new SimpleQueryTree(excludeFromTree(node, tree));
    }

    private static Node excludeFromTree(Node nodeToExclude, Node tree) {
        if (nodeToExclude.equals(tree)) {
            return null;
        }
        switch (tree) {
            case And and -> {
                List<Node> andClause = and.conjuncts()
                        .stream()
                        .map(c -> excludeFromTree(nodeToExclude, c))
                        .filter(Objects::nonNull)
                        .toList();
                return andClause.size() > 1 ? new And(andClause) : andClause.getFirst();
            }
            case Or or -> {
                List<Node> orClause = or.disjuncts()
                        .stream()
                        .map(d -> excludeFromTree(nodeToExclude, d))
                        .filter(Objects::nonNull)
                        .toList();
                return orClause.size() > 1 ? new Or(orClause) : orClause.getFirst();
            }
            case FreeText ignored -> {
                return tree;
            }
            case PropertyValue ignored -> {
                return tree;
            }
        }
    }

    public Set<String> collectGivenTypes() {
        return collectGivenTypes(tree, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case And and -> and.conjuncts().forEach(c -> collectGivenTypes(c, types));
            case Or or -> or.disjuncts().forEach(d -> collectGivenTypes(d, types));
            case PropertyValue pv -> {
                if (List.of("rdf:type").equals(pv.propertyPath())) {
                    types.add(pv.value().string());
                }
            }
            case FreeText ignored -> {
                // Nothing to do here
            }
        }

        return types;
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public boolean isFreeText() {
        return tree instanceof FreeText && ((FreeText) tree).operator().equals(Operator.EQUALS);
    }

    public List<PropertyValue> getTopLevelPvNodes() {
        if (topPvNodes == null) {
            topPvNodes = switch (this.tree) {
                case And and -> and.conjuncts()
                        .stream()
                        .filter(node -> node instanceof PropertyValue)
                        .map(PropertyValue.class::cast)
                        .toList();
                case Or ignored -> Collections.emptyList(); //TODO
                case PropertyValue pv -> List.of(pv);
                case FreeText ignored -> Collections.emptyList();
            };
        }

        return topPvNodes;
    }

    public static PropertyValue pvEqualsLiteral(String property, String value) {
        return new PropertyValue(property, List.of(property), Operator.EQUALS, new Literal(value));
    }

    public static PropertyValue pvEqualsLink(String property, String uri) {
        return new PropertyValue(property, List.of(property), Operator.EQUALS, new Link(uri));
    }

    public static PropertyValue pvEqualsVocabTerm(String property, String value) {
        return new PropertyValue(property, List.of(property), Operator.EQUALS, new VocabTerm(value));
    }

    public String getFreeTextPart() {
        if (freeTextPart == null) {
            if (tree instanceof And) {
                freeTextPart = ((And) tree).conjuncts()
                        .stream()
                        .filter(c -> c instanceof FreeText)
                        .map(FreeText.class::cast)
                        .filter(ft -> ft.operator() == Operator.EQUALS)
                        .map(FreeText::value)
                        .findFirst()
                        .orElse("");
            } else if (isFreeText()) {
                freeTextPart = ((FreeText) tree).value();
            } else {
                freeTextPart = "";
            }
        }

        return freeTextPart;
    }

    public String toQueryString() {
        return buildQueryString(tree, true);
    }

    private String buildQueryString(Node node, boolean topLevel) {
        switch (node) {
            case And and -> {
                String andClause = and.conjuncts()
                        .stream()
                        .map(this::buildQueryString)
                        .collect(Collectors.joining(" "));
                return topLevel ? andClause : "(" + andClause + ")";
            }
            case Or or -> {
                String orClause = or.disjuncts()
                        .stream()
                        .map(this::buildQueryString)
                        .collect(Collectors.joining(" OR "));
                return topLevel ? orClause : "(" + orClause + ")";
            }
            case FreeText ft -> {
                return freeTextToString(ft);
            }
            case PropertyValue pv -> {
                return propertyValueToString(pv);
            }
        }
    }

    private String buildQueryString(Node node) {
        return buildQueryString(node, false);
    }

    private String freeTextToString(FreeText ft) {
        String s = ft.value();
        if (ft.operator() == Operator.NOT_EQUALS) {
            s = "NOT " + s;
        }
        return s;
    }

    private String propertyValueToString(PropertyValue pv) {
        String sep = switch (pv.operator()) {
            case EQUALS, NOT_EQUALS -> ":";
            case GREATER_THAN_OR_EQUALS -> ">=";
            case GREATER_THAN -> ">";
            case LESS_THAN_OR_EQUALS -> "<=";
            case LESS_THAN -> "<";
        };

        String not = pv.operator() == Operator.NOT_EQUALS ? "NOT " : "";
        String path = String.join(".", pv.propertyPath());
        String value = pv.value().string();

        if (pv.value() instanceof Link) {
            value = Disambiguate.toPrefixed(value);
        }

        return not + quoteIfPhraseOrContainsSpecialSymbol(path) + sep + quoteIfPhraseOrContainsSpecialSymbol(value);
    }

    public void replaceTopLevelFreeText(String replacement) {
        if (isFreeText()) {
            this.tree = new FreeText(Operator.EQUALS, replacement);
        } else if (tree instanceof And) {
            List<Node> newConjuncts = ((And) tree).conjuncts().stream()
                    .filter(Predicate.not(c -> c instanceof FreeText && ((FreeText) c).operator().equals(Operator.EQUALS)))
                    .collect(Collectors.toList());
            if (!replacement.isEmpty()) {
                newConjuncts.addFirst(new FreeText(Operator.EQUALS, replacement));
            }
            this.tree = newConjuncts.size() == 1 ? newConjuncts.getFirst() : new And(newConjuncts);
        } else {
            this.tree = new And(List.of(new FreeText(Operator.EQUALS, replacement), tree));
        }
    }
}

package whelk.xlql;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static whelk.search.XLQLQuery.encodeUri;
import static whelk.search.XLQLQuery.quoteIfPhraseOrContainsSpecialSymbol;

import static whelk.xlql.Disambiguate.RDF_TYPE;

public class SimpleQueryTree {
    public sealed interface Node permits And, BoolFilter, FreeText, Or, PropertyValue {
    }

    public record And(List<Node> conjuncts) implements Node {
    }

    public record Or(List<Node> disjuncts) implements Node {
    }

    public record PropertyValue(String property, List<String> path, Operator operator,
                                Value value) implements Node {
        public PropertyValue toOrEquals() {
            return switch (operator()) {
                case GREATER_THAN ->
                        new PropertyValue(property(), path(), Operator.GREATER_THAN_OR_EQUALS, value().increment());
                case LESS_THAN ->
                        new PropertyValue(property(), path(), Operator.LESS_THAN_OR_EQUALS, value().decrement());
                default -> this;
            };
        }

        public String asString(Disambiguate disambiguate) {
            String sep = switch (operator()) {
                case EQUALS, NOT_EQUALS -> ":";
                case GREATER_THAN_OR_EQUALS -> ">=";
                case GREATER_THAN -> ">";
                case LESS_THAN_OR_EQUALS -> "<=";
                case LESS_THAN -> "<";
            };

            String not = operator() == Operator.NOT_EQUALS ? "NOT " : "";
            String path = String.join(".", path());
            String value = value().string();

            if (value() instanceof Link) {
                value = Disambiguate.toPrefixed(value);
            }

            if (path().size() == 1) {
                path = disambiguate.getQueryCode(property()).orElse(path);
            }

            return not + quoteIfPhraseOrContainsSpecialSymbol(path) + sep + quoteIfPhraseOrContainsSpecialSymbol(value);
        }
    }

    public record FreeText(Operator operator, String value) implements Node {
        public String asString() {
            String s = value();
            if (operator() == Operator.NOT_EQUALS) {
                s = "NOT " + s;
            }
            return s;
        }
    }

    public record BoolFilter(String alias, FilterStatus status, Node filter) implements Node {
        public String asString() {
            String s = alias();
            if (status() == FilterStatus.INACTIVE) {
                s = "NOT " + s;
            }
            return s;
        }
    }

    public sealed interface Value permits Link, Literal, VocabTerm {
        String string();

        default boolean isNumeric() {
            return false;
        }

        default Value increment() {
            return this;
        }

        default Value decrement() {
            return this;
        }
    }

    public record Literal(String string) implements Value {
        public boolean isNumeric() {
            return string().matches("\\d+");
        }

        public Literal increment() {
            return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) + 1)) : this;
        }

        public Literal decrement() {
            return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) - 1)) : this;
        }
    }

    public record Link(String string) implements Value {
    }

    public record VocabTerm(String string) implements Value {
    }

    public Node tree;

    private List<PropertyValue> topPvNodes;

    private String freeTextPart;

    public SimpleQueryTree(FlattenedAst ast, Disambiguate disambiguate) throws InvalidQueryException {
        this(ast, disambiguate, Collections.emptyMap());
    }

    public SimpleQueryTree(FlattenedAst ast, Disambiguate disambiguate, Map<String, Map<String, Object>> defaultBoolFilters) throws InvalidQueryException {
        this.tree = buildTree(ast.tree, disambiguate, defaultBoolFilters);
        normalizeFreeText();
    }

    public SimpleQueryTree(Node tree) {
        this.tree = tree;
    }

    private static Node buildTree(FlattenedAst.Node ast, Disambiguate disambiguate, Map<String, Map<String, Object>> defaultBoolFilters) throws InvalidQueryException {
        switch (ast) {
            case FlattenedAst.And and -> {
                List<Node> conjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : and.operands()) {
                    var c = buildTree(o, disambiguate, defaultBoolFilters);
                    if (c != null) {
                        conjuncts.add(c);
                    }
                }
                return switch (conjuncts.size()) {
                    case 0 -> null;
                    case 1 -> conjuncts.getFirst();
                    default -> new And(conjuncts);
                };
            }
            case FlattenedAst.Or or -> {
                List<Node> disjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : or.operands()) {
                    var d = buildTree(o, disambiguate, defaultBoolFilters);
                    if (d != null) {
                        disjuncts.add(d);
                    }
                }
                return switch (disjuncts.size()) {
                    case 0 -> null;
                    case 1 -> disjuncts.getFirst();
                    default -> new Or(disjuncts);
                };
            }
            case FlattenedAst.Not not -> {
                if (defaultBoolFilters.containsKey(not.value())) {
                    return defaultBoolFilters.get(not.value()).get("status").equals(FilterStatus.INACTIVE)
                            ? null
                            : new BoolFilter(not.value(), FilterStatus.INACTIVE, (Node) defaultBoolFilters.get(not.value()).get("filter"));
                }
                return new FreeText(Operator.NOT_EQUALS, not.value());
            }
            case FlattenedAst.Leaf l -> {
                if (defaultBoolFilters.containsKey(l.value())) {
                    return defaultBoolFilters.get(l.value()).get("status").equals(FilterStatus.ACTIVE)
                            ? null
                            : new BoolFilter(l.value(), FilterStatus.ACTIVE, (Node) defaultBoolFilters.get(l.value()).get("filter"));
                }
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

    private void normalizeFreeText() {
        this.tree = normalizeFreeText(tree);
    }

    private static Node normalizeFreeText(Node node) {
        return switch (node) {
            case And and -> {
                List<Node> conjuncts = new ArrayList<>();
                List<String> ftStrings = new ArrayList<>();
                for (Node n : and.conjuncts()) {
                    if (isFreeText(n)) {
                        ftStrings.add(quoteIfPhraseOrContainsSpecialSymbol(((FreeText) n).value()));
                    } else {
                        conjuncts.add(normalizeFreeText(n));
                    }
                }
                if (!ftStrings.isEmpty()) {
                    conjuncts.addFirst(new FreeText(Operator.EQUALS, String.join(" ", ftStrings)));
                }
                yield conjuncts.size() > 1 ? new And(conjuncts) : conjuncts.getFirst();
            }
            case Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(SimpleQueryTree::normalizeFreeText)
                        .toList();
                yield new Or(disjuncts);
            }
            case FreeText ft -> new FreeText(ft.operator(), quoteIfPhraseOrContainsSpecialSymbol(ft.value()));
            case null, default -> node;
        };
    }

    public SimpleQueryTree expandActiveBoolFilters() {
        return new SimpleQueryTree(expandActiveBoolFilters(tree));
    }

    private static Node expandActiveBoolFilters(Node node) {
        return switch (node) {
            case And and -> {
                var conjuncts = and.conjuncts()
                        .stream()
                        .map(SimpleQueryTree::expandActiveBoolFilters)
                        .filter(Objects::nonNull)
                        .toList();
                yield switch (conjuncts.size()) {
                    case 0 -> null;
                    case 1 -> conjuncts.getFirst();
                    default -> new And(conjuncts);
                };
            }
            case BoolFilter bf -> switch (bf.status()) {
                case INACTIVE -> null;
                case ACTIVE -> bf.filter();
            };
            default -> node;
        };
    }

    public SimpleQueryTree andExtend(Node node) {
        var conjuncts = switch (tree) {
            case null -> List.of(node);
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
        return switch (tree) {
            case And and -> {
                List<Node> andClause = and.conjuncts()
                        .stream()
                        .map(c -> excludeFromTree(nodeToExclude, c))
                        .filter(Objects::nonNull)
                        .toList();
                yield andClause.size() > 1 ? new And(andClause) : andClause.getFirst();
            }
            case Or or -> {
                List<Node> orClause = or.disjuncts()
                        .stream()
                        .map(d -> excludeFromTree(nodeToExclude, d))
                        .filter(Objects::nonNull)
                        .toList();
                yield orClause.size() > 1 ? new Or(orClause) : orClause.getFirst();
            }
            default -> tree;
        };
    }

    public SimpleQueryTree removeTopLevelRangeNodes(String property) {
        var rangeOps = Set.of(Operator.GREATER_THAN_OR_EQUALS, Operator.GREATER_THAN, Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS);
        return new SimpleQueryTree(removeTopLevelPvNodes(property, tree, rangeOps));
    }

    public SimpleQueryTree removeTopLevelPvNodes(String property) {
        return new SimpleQueryTree(removeTopLevelPvNodes(property, tree, Collections.emptySet()));
    }

    private static Node removeTopLevelPvNodes(String property, Node tree, Set<Operator> operators) {
        Predicate<Node> p = (node -> node instanceof PropertyValue
                && ((PropertyValue) node).property().equals(property)
                && (operators.isEmpty() || operators.contains(((PropertyValue) node).operator())));

        return switch (tree) {
            case null -> null;
            case And and -> {
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .filter(Predicate.not(p))
                        .collect(Collectors.toList());
                if (conjuncts.isEmpty()) {
                    yield null;
                } else if (conjuncts.size() == 1) {
                    yield conjuncts.getFirst();
                } else {
                    yield new And(conjuncts);
                }
            }
            default -> p.test(tree) ? null : tree;
        };
    }

    public Set<String> collectGivenTypes() {
        return collectGivenTypes(tree, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case And and -> and.conjuncts().forEach(c -> collectGivenTypes(c, types));
            case Or or -> or.disjuncts().forEach(d -> collectGivenTypes(d, types));
            case PropertyValue pv -> {
                if (List.of(RDF_TYPE).equals(pv.path())) {
                    types.add(pv.value().string());
                }
            }
            default -> {
                // Nothing to do here
            }
        }

        return types;
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public boolean isFreeText() {
        return isFreeText(tree);
    }

    private static boolean isFreeText(Node node) {
        return node instanceof FreeText && ((FreeText) node).operator().equals(Operator.EQUALS);
    }

    public Set<String> getBoolFilterAliases() {
        return getBoolFiltersByAlias().keySet();
    }

    public Map<String, BoolFilter> getBoolFiltersByAlias() {
        Map<String, BoolFilter> bfByAlias = new HashMap<>();

        switch (tree) {
            case And and -> and.conjuncts().forEach(node -> {
                if (node instanceof BoolFilter n) {
                    bfByAlias.put(n.alias(), n);
                }
            });
            case BoolFilter bf -> bfByAlias.put(bf.alias(), bf);
            case null, default ->  {
            }
        }

        return bfByAlias;
    }

    public List<PropertyValue> getTopLevelPvNodes() {
        if (topPvNodes == null) {
            topPvNodes = switch (tree) {
                case And and -> and.conjuncts()
                        .stream()
                        .filter(node -> node instanceof PropertyValue)
                        .map(PropertyValue.class::cast)
                        .toList();
                case Or ignored -> Collections.emptyList(); //TODO
                case PropertyValue pv -> List.of(pv);
                case null, default -> Collections.emptyList();
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

    public String toQueryString(Disambiguate disambiguate) {
        return isEmpty() ? "*" : buildQueryString(tree, disambiguate, true);
    }

    private String buildQueryString(Node node, Disambiguate disambiguate, boolean topLevel) {
        return switch (node) {
            case And and -> {
                String andClause = and.conjuncts()
                        .stream()
                        .map(n -> buildQueryString(n, disambiguate, false))
                        .collect(Collectors.joining(" "));
                yield topLevel ? andClause : "(" + andClause + ")";
            }
            case Or or -> {
                String orClause = or.disjuncts()
                        .stream()
                        .map(n -> buildQueryString(n, disambiguate, false))
                        .collect(Collectors.joining(" OR "));
                yield topLevel ? orClause : "(" + orClause + ")";
            }
            case FreeText ft -> ft.asString();
            case PropertyValue pv -> pv.asString(disambiguate);
            case BoolFilter bf -> bf.asString();
        };
    }
}

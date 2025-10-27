package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.Operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.search2.EsMappings.KEYWORD;
import static whelk.search2.EsMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.EsMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.Operator.EQUALS;
import static whelk.search2.Operator.GREATER_THAN;
import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.nestedWrap;
import static whelk.search2.QueryUtil.parenthesize;

public sealed class PathValue implements Node permits Type {
    private final Path path;
    private final Operator operator;
    private final Value value;

    public PathValue(Path path, Operator operator, Value value) {
        this.path = path;
        this.operator = operator;
        this.value = value;
    }

    public PathValue(Property property, Operator operator, Value value) {
        this(new Path(property), operator, value);
    }

    public PathValue(String key, Operator operator, Value value) {
        this(new Path.ExpandedPath(new Key.RecognizedKey(key)), operator, value);
    }

    public Path path() {
        return path;
    }

    public Operator operator() {
        return operator;
    }

    public Value value() {
        return value;
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        if (value instanceof FreeText ft) {
            // FIXME: This is only needed until frontend no longer rely on quoted values not being treated as such.
            List<Token> unquotedTokens = ft.tokens().stream()
                    .map(t -> t.isQuoted() ? new Token.Raw(t.value(), t.offset()) : t)
                    .toList();
            FreeText newFt = new FreeText(ft.textQuery(), unquotedTokens, ft.connective());
            PathValue newPv = new PathValue(path, operator, newFt);
            return newPv.getEsNestedQuery(esSettings)
                    .orElse(newPv.getCoreEsQuery(esSettings));
        }
        return getEsNestedQuery(esSettings)
                .orElse(getCoreEsQuery(esSettings));
    }

    public Map<String, Object> getCoreEsQuery(ESSettings esSettings) {
        return _getCoreEsQuery(esSettings);
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> subjectTypes) {
        return path.isValid() ? _expand(jsonLd, subjectTypes) : this;
    }

    public Node expand(JsonLd jsonLd) {
        return _expand(jsonLd, List.of());
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        return _toSearchMapping(makeUpLink);
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return operator.format(path.queryForm(), value.isMultiToken() ? parenthesize(value.queryForm()) : value.queryForm());
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public Node getInverse() {
        return operator.isRange() ? withOperator(operator.getInverse()) : new Not(this);
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return this;
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return implies(node, this::equals);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PathValue other && hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, operator, value);
    }

    public boolean isTypeNode() {
        return getSoleProperty().filter(Property::isRdfType).isPresent() && operator.equals(EQUALS) && value instanceof VocabTerm;
    }

    public Type asTypeNode() {
        return new Type((Property.RdfType) getSoleProperty().get(), (VocabTerm) value());
    }

    public boolean hasEqualProperty(Property property) {
        return getSoleProperty().filter(property::equals).isPresent();
    }

    public PathValue withOperator(Operator replacement) {
        return new PathValue(path, replacement, value);
    }

    public PathValue withValue(Value replacement) {
        return new PathValue(path, operator, replacement);
    }

    public PathValue withPath(Path path) {
        return new PathValue(path, operator, value);
    }

    public PathValue toOrEquals() {
        if (value instanceof Numeric n) {
            if (operator.equals(GREATER_THAN)) {
                return new PathValue(path, Operator.GREATER_THAN_OR_EQUALS, n.increment());
            }
            if (operator.equals(Operator.LESS_THAN)) {
                return new PathValue(path, Operator.LESS_THAN_OR_EQUALS, n.decrement());
            }
        }
        return this;
    }

    public Optional<Property> getSoleProperty() {
        return path.path().size() == 1 && path.first() instanceof Property p
                ? Optional.of(p)
                : Optional.empty();
    }

    private Optional<Key> getSoleKey() {
        return path.path().size() == 1 && path.first() instanceof Key k
                ? Optional.of(k)
                : Optional.empty();
    }

    private Optional<Map<String, Object>> getEsNestedQuery(ESSettings esSettings) {
        return path.getEsNestedStem(esSettings.mappings())
                .map(nestedStem -> nestedWrap(nestedStem, getCoreEsQuery(esSettings)));
    }

    private Map<String, Object> _getCoreEsQuery(ESSettings esSettings) {
        String field = path.jsonForm();
        return switch (value) {
            case DateTime dateTime -> esDateFilter(field, dateTime, esSettings);
            case FreeText ft -> esFreeTextFilter(field, ft, esSettings);
            case InvalidValue ignored -> nonsenseFilter(); // TODO: Treat whole expression as free text?
            case Numeric numeric -> esNumFilter(field, numeric, esSettings);
            case Resource resource -> esResourceFilter(field, resource);
            case Term term -> esTermFilter(field, term);
        };
    }

    private Map<String, Object> esDateFilter(String field, DateTime d, ESSettings esSettings) {
        // TODO: What about e.g. :firstIssueDate/:lastIssueDate? These have range xsd:date however are not indexed as date type in ES.
        if (esSettings.mappings().isDateTypeField(field)) {
            return esNumOrDateFilter(field, d.dateTime().toElasticDateString());
        }
        // Treat as free text
        return esFreeTextFilter(field, new FreeText(d.toString()), esSettings);
    }

    private Map<String, Object> esNumFilter(String field, Numeric n, ESSettings esSettings) {
        EsMappings esMappings = esSettings.mappings();

        // Known placeholder values (0000, 9999) are excluded from 4-digit fields to prevent them from being treated as valid years in sorting and aggregations.
        Predicate<String> isFourDigitsFieldValue = s -> s.length() == 4 && !s.equals("0000") && !s.equals("9999");

        if (operator.isRange() && esMappings.hasFourDigitsShortField(field)) {
            return esNumOrDateFilter(field + FOUR_DIGITS_SHORT_SUFFIX, n.value());
        }
        if (!operator.isRange()) {
            if (esMappings.hasFourDigitsKeywordField(field) && isFourDigitsFieldValue.test(n.toString())) {
                return esNumOrDateFilter(field + FOUR_DIGITS_KEYWORD_SUFFIX, n.toString());
            }
            if (esMappings.hasKeywordSubfield(field)) {
                return esTermQueryFilter(String.format("%s.%s", field, KEYWORD), n.toString());
            }
        }
        if (esMappings.isLongTypeField(field)) {
            return esNumOrDateFilter(field, n.value());
        }

        // Treat as free text
        return esFreeTextFilter(field, new FreeText(n.toString()), esSettings);
    }

    private Map<String, Object> esNumOrDateFilter(String f, Object v) {
        return switch (operator) {
            case EQUALS -> esTermQueryFilter(f, v);
            case GREATER_THAN_OR_EQUALS -> esRangeFilter(f, v, "gte");
            case GREATER_THAN -> esRangeFilter(f, v, "gt");
            case LESS_THAN_OR_EQUALS -> esRangeFilter(f, v, "lte");
            case LESS_THAN -> esRangeFilter(f, v, "lt");
        };
    }

    private Map<String, Object> esFreeTextFilter(String f, FreeText ft, ESSettings esSettings) {
        if (ft.isWild()) {
            if (operator.isRange()) {
                // FIXME: Range makes no sense here
                return nonsenseFilter();
            }
            return existsFilter(f);
        }

        var boostSettings = esSettings.boost().fieldBoost();

        if (operator.isRange()) {
            // FIXME: Range makes no sense here
            return nonsenseFilter();
        }

        return ft.toEs(boostSettings.withField(f));
    }

    private Map<String, Object> esResourceFilter(String f, Resource r) {
        if (operator.isRange()) {
            // FIXME: Range makes no sense here
            return nonsenseFilter();
        }
        return esTermQueryFilter(f, r.jsonForm());
    }

    private Map<String, Object> esTermFilter(String f, Term t) {
        if (operator.isRange()) {
            // FIXME: Range makes no sense here
            return nonsenseFilter();
        }
        return esTermQueryFilter(f, t.term());
    }

    private Map<String, Object> _toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        Map<String, Object> m = new LinkedHashMap<>();

        var propertyChainAxiom = new LinkedList<>();
        for (int i = path.path().size() - 1; i >= 0; i--) {
            Subpath sp = path.path().get(i);
            if (!sp.isValid()) {
                propertyChainAxiom.push(Map.of(TYPE_KEY, "_Invalid", "label", sp.queryForm()));
                continue;
            }
            if (sp instanceof Property p) {
                propertyChainAxiom.push(i > 0 && path.path().get(i - 1).queryForm().equals(JsonLd.REVERSE_KEY)
                        ? Map.of(INVERSE_OF, p.definition())
                        : p.definition());
            }
        }
        var property = switch (propertyChainAxiom.size()) {
            case 0 -> Map.of();
            case 1 -> propertyChainAxiom.pop();
            default -> Map.of(PROPERTY_CHAIN_AXIOM, propertyChainAxiom);
        };
        m.put("property", property);
        m.put(operator.termKey, value instanceof Resource r ? r.description() : value.queryForm());
        m.put("up", makeUpLink.apply(this));

        m.put("_key", path.queryForm());
        m.put("_value", value.queryForm());

        return m;
    }

    private Node _expand(JsonLd jsonLd, Collection<String> subjectTypes) {
        Path.ExpandedPath expandedPath = path.expand(jsonLd, value);


        if (!subjectTypes.isEmpty()) {
            List<Path.ExpandedPath> altPaths = expandedPath.getAltPaths(jsonLd, subjectTypes);
            List<Path.ExpandedPath> alt2Paths = expandedPath.getAlt2Paths(jsonLd);
            var altPvNodes = Stream.concat(altPaths.stream(), alt2Paths.stream())
                    .map(this::withPath)
                    .map(pv -> pv.expand(jsonLd))
                    .toList();
            return altPaths.size() > 1 ? new Or(altPvNodes) : altPvNodes.getFirst();
        }

        List<Node> prefilledFields = getPrefilledFields(expandedPath.path(), jsonLd);

        // When querying type, match any subclass by default (TODO: make this optional)
        Node expanded = withPath(expandedPath).expandType(jsonLd);

        return prefilledFields.isEmpty() ? expanded : new And(Stream.concat(Stream.of(expanded), prefilledFields.stream()).toList());
    }

    private List<Node> getPrefilledFields(List<Subpath> path, JsonLd jsonLd) {
        List<Node> prefilledFields = new ArrayList<>();
        List<Subpath> currentPath = new ArrayList<>();
        for (Subpath sp : path) {
            currentPath.add(sp);
            if (sp instanceof Property p) {
                for (Property.Restriction r : p.restrictions()) {
                    List<Subpath> restrictedPath = Stream.concat(currentPath.stream(), Stream.of(r.property())).toList();
                    Node expanded = new PathValue(new Path(restrictedPath).expand(jsonLd, r.value()), EQUALS, r.value()).expandType(jsonLd);
                    prefilledFields.add(expanded);
                }
            }
        }
        return prefilledFields;
    }

    // When querying type, match any subclass by default (TODO: make this optional)
    private Node expandType(JsonLd jsonLd) {
        if (!path.last().isType() || !(value instanceof VocabTerm)) {
            return this;
        }

        String baseType = ((VocabTerm) value).key();

        Set<String> subtypes = jsonLd.getSubClasses(baseType);
        if (subtypes.isEmpty()) {
            return this;
        }

        List<Node> altFields = Stream.concat(Stream.of(baseType), subtypes.stream())
                .sorted()
                .map(t -> (Node) withValue(new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return new Or(altFields);
    }

    private static Map<String, Object> esRangeFilter(String path, Object value, String key) {
        return filterWrap(rangeWrap(Map.of(path, Map.of(key, value))));
    }

    private static Map<String, Object> existsFilter(String path) {
        return Map.of("exists", Map.of("field", path));
    }

    private static Map<String, Object> filterWrap(Map<?, ?> m) {
        return boolWrap(Map.of("filter", m));
    }

    private static Map<String, Object> rangeWrap(Map<?, ?> m) {
        return Map.of("range", m);
    }

    private static Map<String, Object> esTermQueryFilter(String field, Object value) {
        return filterWrap(Map.of("term", Map.of(field, value)));
    }

    // FIXME: Handle queries that are syntactically correct but make no sense and are guaranteed to return no hits
    private static Map<String, Object> nonsenseFilter() {
        return existsFilter("nonsense.field");
    }
}
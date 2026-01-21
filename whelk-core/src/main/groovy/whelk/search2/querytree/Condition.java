package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.hash;
import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.SEARCH_KEY;
import static whelk.search2.EsMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.EsMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.EsMappings.KEYWORD;
import static whelk.search2.Operator.EQUALS;
import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.nestedWrap;
import static whelk.search2.QueryUtil.parenthesize;

public sealed class Condition implements Node permits Type {
    private final Selector selector;
    private final Operator operator;
    private final Value value;

    public Condition(Selector selector, Operator operator, Value value) {
        this.selector = selector;
        this.operator = operator;
        this.value = value;
    }

    public Condition(String key, Operator operator, Value value) {
        this(new Key.RecognizedKey(new Token.Raw(key)), operator, value);
    }

    public Selector selector() {
        return selector;
    }

    public Operator operator() {
        return operator;
    }

    public Value value() {
        return value;
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return getEsNestedQuery(esSettings).orElse(getCoreEsQuery(esSettings));
    }

    public Map<String, Object> getCoreEsQuery(ESSettings esSettings) {
        return _getCoreEsQuery(selector.esField(), value, esSettings);
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return selector.isValid()
                ? expandWithAltSelectors(jsonLd, rdfSubjectTypes)
                : ExpandedNode.identity(this);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        return _toSearchMapping(makeUpLink);
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return operator.format(selector.queryKey(), value.isMultiToken() ? parenthesize(value.queryForm()) : value.queryForm());
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
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Condition other && hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        return hash(selector, operator, value);
    }

    public Condition withSelector(Selector s) {
        return new Condition(s, operator, value);
    }

    public Condition withOperator(Operator op) {
        return new Condition(selector, op, value);
    }

    public Condition withValue(Value v) {
        return new Condition(selector, operator, v);
    }

    public boolean isTypeNode() {
        return selector instanceof Property.RdfType && operator.equals(EQUALS) && value instanceof VocabTerm;
    }

    public Type asTypeNode() {
        return new Type((Property.RdfType) selector, (VocabTerm) value);
    }

    private ExpandedNode expandWithAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        List<Node> withAltSelectors = selector.getAltSelectors(jsonLd, rdfSubjectTypes).stream()
                .map(s -> s.withPrependedMetaProperty(jsonLd))
                .map(this::withSelector)
                .map(s -> s._expand(jsonLd))
                .toList();
        Node expanded = withAltSelectors.size() > 1 ? new Or.AltSelectors(withAltSelectors, selector) : withAltSelectors.getFirst();
        return new ExpandedNode(expanded, Map.of(this, expanded));
    }

    private Node _expand(JsonLd jsonLd) {
        List<? extends PathElement> path = selector.path();

        List<Node> expanded = Stream.concat(Stream.of(withSelector(path.size() > 1 ? new Path(path) : path.getFirst())), getPrefilledFields(path).stream())
                .map(s -> s.expandType(jsonLd))
                .toList();

        return expanded.size() > 1 ? new And(expanded) : expanded.getFirst();
    }

    private List<Condition> getPrefilledFields(List<? extends PathElement> path) {
        List<Condition> prefilledFields = new ArrayList<>();
        List<PathElement> currentPath = new ArrayList<>();
        for (PathElement pe : path) {
            currentPath.add(pe);
            if (pe instanceof Property p && p.isRestrictedSubProperty() && !p.hasIndexKey()) {
                for (Restrictions.OnProperty r : p.objectOnPropertyRestrictions()) {
                    // Support only HasValue restriction for now
                    if (r instanceof Restrictions.HasValue(Property property, Value v)) {
                        var restrictedPath = new Path(Stream.concat(currentPath.stream(), property.path().stream()).toList());
                        prefilledFields.add(new Condition(restrictedPath, EQUALS, v));
                    }
                }
            }
        }
        return prefilledFields;
    }

    // When querying type, match any subclass by default (TODO: make this optional)
    private Node expandType(JsonLd jsonLd) {
        if (!(selector.isType() && value instanceof VocabTerm v)) {
            return this;
        }

        String baseType = v.key();

        Set<String> subtypes = jsonLd.getSubClasses(baseType);
        if (subtypes.isEmpty()) {
            return this;
        }

        List<Condition> altFields = Stream.concat(Stream.of(baseType), subtypes.stream())
                .sorted()
                .map(t -> withValue(new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return new Or(altFields);
    }

    private Map<String, Object> _toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("property", selector.definition());
        m.put(operator.termKey, value instanceof Resource r ? r.description() : value.queryForm());
        m.put("up", makeUpLink.apply(this));

        m.put("_key", selector.queryKey());
        m.put("_value", value.queryForm());

        return m;
    }

    private Map<String, Object> _getCoreEsQuery(String f, Value v, ESSettings esSettings) {
        if (operator.isRange() && !v.isRangeOpCompatible()) {
            // FIXME
            return nonsenseFilter();
        }
        return switch (v) {
            case DateTime dateTime -> esDateFilter(f, dateTime, esSettings);
            case FreeText ft -> ft.isWild()
                    ? existsFilter(f)
                    : esFreeTextFilter(selector.isObjectProperty() ? f + "." + SEARCH_KEY : f, ft, esSettings);
            case InvalidValue ignored -> nonsenseFilter(); // TODO: Treat whole expression as free text?
            case Numeric numeric -> esNumFilter(f, numeric, esSettings);
            case Link link -> esResourceFilter(selector.isObjectProperty() ? f + "." + ID_KEY : f, link);
            case VocabTerm vocabTerm -> esResourceFilter(f, vocabTerm);
            case Term term -> esTermFilter(f, term);
            case YearRange yearRange -> esYearRangeFilter(f, yearRange, esSettings);
        };
    }

    private Optional<Map<String, Object>> getEsNestedQuery(ESSettings esSettings) {
        return selector.getEsNestedStem(esSettings.mappings())
                .map(nestedStem -> nestedWrap(nestedStem, getCoreEsQuery(esSettings)));
    }

    private Map<String, Object> esDateFilter(String field, DateTime d, ESSettings esSettings) {
        // TODO: What about e.g. :firstIssueDate/:lastIssueDate? These have range xsd:date however are not indexed as date type in ES.
        if (esSettings.mappings().isDateTypeField(field)) {
            return esNumOrDateFilter(field, d.dateTime().toElasticDateString());
        }
        // Treat as free text
        return _getCoreEsQuery(field, new FreeText(d.toString()), esSettings);
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
        return _getCoreEsQuery(field, new FreeText(n.toString()), esSettings);
    }

    private Map<String, Object> esYearRangeFilter(String field, YearRange yearRange, ESSettings esSettings) {
        EsMappings esMappings = esSettings.mappings();

        if (esMappings.hasFourDigitsShortField(field)) {
            return esRangeFilter(field + FOUR_DIGITS_SHORT_SUFFIX, yearRange.toEsIntRange());
        }

        if (esMappings.isDateTypeField(field)) {
            return esRangeFilter(field, yearRange.toEsDateRange());
        }

        return _getCoreEsQuery(field, new FreeText(yearRange.toString()), esSettings);
    }

    private Map<String, Object> esNumOrDateFilter(String f, Object v) {
        return switch (operator) {
            case EQUALS -> esTermQueryFilter(f, v);
            case GREATER_THAN_OR_EQUALS -> esRangeFilter(f, "gte", v);
            case GREATER_THAN -> esRangeFilter(f, "gt", v);
            case LESS_THAN_OR_EQUALS -> esRangeFilter(f, "lte", v);
            case LESS_THAN -> esRangeFilter(f, "lt", v);
        };
    }

    private Map<String, Object> esFreeTextFilter(String f, FreeText ft, ESSettings esSettings) {
        var boostSettings = esSettings.boost().fieldBoost();
        return ft.toEs(boostSettings.withField(f));
    }

    private Map<String, Object> esResourceFilter(String f, Resource r) {
        return esTermQueryFilter(f, r.jsonForm());
    }

    private Map<String, Object> esTermFilter(String f, Term t) {
        return esTermQueryFilter(f, t.term());
    }

    private static Map<String, Object> esRangeFilter(String field, String esRangeOp, Object value) {
        return esRangeFilter(field, Map.of(esRangeOp, value));
    }

    private static Map<String, Object> esRangeFilter(String field, Map<String, Object> esRangeObj) {
        return filterWrap(rangeWrap(Map.of(field, esRangeObj)));
    }

    private static Map<String, Object> existsFilter(String field) {
        return Map.of("exists", Map.of("field", field));
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

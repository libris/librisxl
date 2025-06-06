package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.EsBoost;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.search2.Operator.GREATER_THAN;
import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.nestedWrap;
import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;

public record PathValue(Path path, Operator operator, Value value) implements Node {
    public PathValue(Property property, Operator operator, Value value) {
        this(new Path(property), operator, value);
    }

    public PathValue(String key, Operator operator, Value value) {
        this(new Path.ExpandedPath(new Key.RecognizedKey(key)), operator, value);
    }

    @Override
    public Map<String, Object> toEs(EsMappings esMappings, EsBoost.Config boostConfig) {
        var es = getCoreEsQuery(esMappings);
        return getEsNestedQuery(es, esMappings).orElse(es);
    }

    public Map<String, Object> getCoreEsQuery(EsMappings esMappings) {
        return _getCoreEsQuery(esMappings);
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return path.isValid() ? _expand(jsonLd, rulingTypes) : this;
    }

    public Node expand(JsonLd jsonLd) {
        return _expand(jsonLd, List.of());
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        return _toSearchMapping(qt, queryParams);
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return toRawQueryString();
    }

    @Override
    public Node getInverse() {
        return new PathValue(path, operator.getInverse(), value);
    }

    @Override
    public boolean shouldContributeToEsScore() {
        return value instanceof Literal l && !l.isWildcard();
    }

    private String toRawQueryString() {
        return format(path.asKey(), value.raw());
    }

    @Override
    public String toString() {
        return format(path.toString(), value.toString());
    }

    @Override
    public boolean isTypeNode() {
        return (getSoleProperty().filter(Property::isRdfType).isPresent() || getSoleKey().filter(Key::isType).isPresent())
                && operator.equals(Operator.EQUALS);
    }

    public boolean hasEqualProperty(Property property) {
        return getSoleProperty().filter(property::equals).isPresent();
    }

    public boolean hasEqualProperty(String propertyKey) {
        return getSoleProperty().map(Property::name).filter(propertyKey::equals).isPresent();
    }

    public PathValue replaceOperator(Operator replacement) {
        return new PathValue(path, replacement, value);
    }

    public PathValue toOrEquals() {
        if (value instanceof Literal l) {
            if (operator.equals(GREATER_THAN)) {
                return new PathValue(path, Operator.GREATER_THAN_OR_EQUALS, l.increment());
            }
            if (operator.equals(Operator.LESS_THAN)) {
                return new PathValue(path, Operator.LESS_THAN_OR_EQUALS, l.decrement());
            }
        }
        return this;
    }

    private Optional<Property> getSoleProperty() {
        return path.path().size() == 1 && path.first() instanceof Property p
                ? Optional.of(p)
                : Optional.empty();
    }

    private Optional<Key> getSoleKey() {
        return path.path().size() == 1 && path.first() instanceof Key k
                ? Optional.of(k)
                : Optional.empty();
    }

    private Optional<Map<String, Object>> getEsNestedQuery(Map<String, Object> esCoreQuery, EsMappings esMappings) {
        return path.getEsNestedStem(esMappings)
                .map(nestedStem -> nestedWrap(nestedStem, esCoreQuery))
                .map(operator == Operator.NOT_EQUALS ? QueryUtil::mustNotWrap : QueryUtil::mustWrap);
    }

    private Map<String, Object> _getCoreEsQuery(EsMappings esMappings) {
        String p = path.fullEsSearchPath();
        String v = value.jsonForm();

        if (Operator.WILDCARD.equals(v)) {
            return switch (operator) {
                case EQUALS -> existsFilter(p);
                case NOT_EQUALS -> notExistsFilter(p);
                default -> notExistsFilter(p); // TODO?
            };
        }

        return switch (operator) {
            case EQUALS -> esEquals(p, v, esMappings);
            case NOT_EQUALS -> esNotEquals(p, v);
            case LESS_THAN -> esRangeFilter(p, v, "lt");
            case LESS_THAN_OR_EQUALS -> esRangeFilter(p, v, "lte");
            case GREATER_THAN -> esRangeFilter(p, v, "gt");
            case GREATER_THAN_OR_EQUALS -> esRangeFilter(p, v, "gte");
        };
    }

    private Map<String, Object> _toSearchMapping(QueryTree qt, QueryParams queryParams) {
        Map<String, Object> m = new LinkedHashMap<>();

        var propertyChainAxiom = new LinkedList<>();
        for (int i = path.path().size() - 1; i >= 0; i--) {
            Subpath sp = path.path().get(i);
            if (!sp.isValid()) {
                propertyChainAxiom.push(Map.of(TYPE_KEY, "_Invalid", "label", sp.key().toString()));
                continue;
            }
            if (sp instanceof Property p) {
                propertyChainAxiom.push(i > 0 && path.path().get(i - 1).key().toString().equals(JsonLd.REVERSE_KEY)
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
        m.put(operator.termKey, value.description());
        m.put("up", makeUpLink(qt, this, queryParams));

        m.put("_key", path.asKey());
        m.put("_value", value.raw());

        return m;
    }

    private Node _expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        Path.ExpandedPath expandedPath = path.expand(jsonLd, value);

        if (!rulingTypes.isEmpty()) {
            List<Path.ExpandedPath> altPaths = expandedPath.getAltPaths(jsonLd, rulingTypes);
            var altPvNodes = altPaths.stream()
                    .map(ap -> new PathValue(ap, operator, value).expand(jsonLd))
                    .toList();
            return altPaths.size() > 1
                    ? (operator == Operator.NOT_EQUALS ? new And(altPvNodes) : new Or(altPvNodes))
                    : altPvNodes.getFirst();
        }

        List<Node> prefilledFields = getPrefilledFields(expandedPath.path(), jsonLd);

        // When querying type, match any subclass by default (TODO: make this optional)
        Node expanded = new PathValue(expandedPath, operator, value).expandType(jsonLd);

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
                    Node expanded = new PathValue(new Path(restrictedPath).expand(jsonLd, r.value()), operator, r.value()).expandType(jsonLd);
                    prefilledFields.add(expanded);
                }
            }
        }
        return prefilledFields;
    }

    // When querying type, match any subclass by default (TODO: make this optional)
    private Node expandType(JsonLd jsonLd) {
        if (!path.last().isType()) {
            return this;
        }

        Set<String> subtypes = jsonLd.getSubClasses(value.jsonForm());
        if (subtypes.isEmpty()) {
            return this;
        }

        List<Node> altFields = Stream.concat(Stream.of(value.jsonForm()), subtypes.stream())
                .sorted()
                .map(t -> (Node) new PathValue(path, operator, new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return operator == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    private String format(String path, String value) {
        String p = quoteIfPhraseOrContainsSpecialSymbol(path);
        String v = quoteIfPhraseOrContainsSpecialSymbol(value);
        return operator.format(p, v);
    }

    private Map<String, Object> esEquals(String path, String value, EsMappings esMappings) {
        if (this.value instanceof Resource) {
            return filterWrap(buildTermQuery(path, value));
        }
        var simpleQuery = buildSimpleQuery(path, value);
        if (esMappings.isFourDigitField(path) || esMappings.isDateField(path)) {
            // TODO: Rather search keyword field with a term query?
            return filterWrap(simpleQuery);
        }
        path += ("^" + EsBoost.WITHIN_FIELD_BOOST);
        return mustWrap(buildSimpleQuery(path, value));
    }

    private Map<String, Object> esNotEquals(String path, String value) {
        return mustNotWrap(this.value instanceof Resource
                ? buildTermQuery(path, value)
                : buildSimpleQuery(path, value));
    }

    private static Map<String, Object> esRangeFilter(String path, String value, String key) {
        return filterWrap(rangeWrap(Map.of(path, Map.of(key, value))));
    }

    private static Map<String, Object> notExistsFilter(String path) {
        return mustNotWrap(existsFilter(path));
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

    private static Map<String, Object> buildSimpleQuery(String field, String value) {
        boolean isSimple = QueryUtil.isSimple(value);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        var query = new HashMap<>();
        query.put("query", isSimple ? value : QueryUtil.escapeNonSimpleQueryString(value));
        query.put("fields", List.of(field));
        query.put("default_operator", "AND");
        return Map.of(queryMode, query);
    }

    private static Map<String, Object> buildTermQuery(String field, String value) {
        return Map.of("term", Map.of(field, value));
    }
}
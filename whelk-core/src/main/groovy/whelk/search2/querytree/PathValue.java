package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.nestedWrap;
import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;

public record PathValue(Path path, Operator operator, Value value) implements Node {
    public PathValue(List<Object> path, Operator operator, Value value) {
        this(new Path(path), operator, value);
    }

    public PathValue(String path, Operator operator, Value value) {
        this(new Path(List.of(path)), operator, value);
    }

    public PathValue(String path, Operator operator, String value) {
        this(path, operator, new Literal(value));
    }

    @Override
    public Map<String, Object> toEs(List<String> boostedFields) {
        return path.nestedStem()
                .map(this::toEsNested)
                .orElseGet(this::getEs);
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        Map<String, Object> m = new LinkedHashMap<>();

        var propertyChainAxiom = new LinkedList<>();

        for (int i = getPath().size() - 1; i >= 0; i--) {
            if (getPath().get(i) instanceof Property property) {
                propertyChainAxiom.push(i > 0 && getPath().get(i - 1).equals(JsonLd.REVERSE_KEY)
                        ? Map.of("inverseOf", property.definition())
                        : property.definition());
            } else if (getPath().get(i) instanceof InvalidKey invalid) {
                propertyChainAxiom.push(invalid.getDefinition());
            }
        }

        if (propertyChainAxiom.size() == 1) {
            m.put("property", propertyChainAxiom.getFirst());
        } else if (propertyChainAxiom.size() > 1) {
            m.put("property", Map.of("propertyChainAxiom", propertyChainAxiom));
        }

        m.put(operator.termKey, value.description());
        m.put("up", qt.makeUpLink(this, nonQueryParams));

        return m;
    }

    @Override
    public String toString(boolean topLevel) {
        return asString();
    }

    @Override
    public Node insertOperator(Operator o) {
        if (operator != null) {
            throw new UnsupportedOperationException("Operator already exists");
        }
        return new PathValue(path, o, value);
    }

    @Override
    public Node insertValue(Value v) {
        return value == null ? new PathValue(path, operator, v) : this;
    }

    @Override
    public Node insertNested(Function<String, Optional<String>> getNestedPath) {
        return new PathValue(path.insertNested(getNestedPath), operator, value);
    }

    @Override
    public Node modifyAllPathValue(Function<PathValue, PathValue> modifier) {
        return modifier.apply(this);
    }

    public boolean isNested() {
        return path.nestedStem().isPresent();
    }

    public List<Object> getPath() {
        return path.path();
    }

    public Optional<String> getNestedStem() {
        return path.nestedStem();
    }

    private String asString() {
        String p = quoteIfPhraseOrContainsSpecialSymbol(path.toString());
        String v = quoteIfPhraseOrContainsSpecialSymbol(value.canonicalForm());
        return operator.format(p, v);
    }

    public Map<String, Object> getEs() {
        var p = getFullSearchPath();
        var v = value.string();

        if (Operator.WILDCARD.equals(v)) {
            return switch (operator) {
                case EQUALS -> existsFilter(p);
                case NOT_EQUALS -> notExistsFilter(p);
                default -> notExistsFilter(p); // TODO?
            };
        }

        return switch (operator) {
            case EQUALS -> equalsFilter(p, v);
            case NOT_EQUALS -> isNested() ? equalsFilter(p, v) : notEqualsFilter(p, v);
            case LESS_THAN -> rangeFilter(p, v, "lt");
            case LESS_THAN_OR_EQUALS -> rangeFilter(p, v, "lte");
            case GREATER_THAN -> rangeFilter(p, v, "gt");
            case GREATER_THAN_OR_EQUALS -> rangeFilter(p, v, "gte");
        };
    }

    private Map<String, Object> toEsNested(String nestedPath) {
        return operator == Operator.NOT_EQUALS
                ? mustNotWrap(nestedWrap(nestedPath, getEs()))
                : mustWrap(nestedWrap(nestedPath, getEs()));
    }

    public PathValue prepend(List<Object> subpath) {
        return new PathValue(path.prepend(subpath), operator, value);
    }

    public PathValue prepend(Object subpath) {
        return prepend(List.of(subpath));
    }

    public String getFullSearchPath() {
        return path.hasIdOrSearchKey()
                ? path.toString()
                : getSuffix().map(path::append).orElse(path).toString();
    }

    private Optional<String> getSuffix() {
        return path.mainProperty()
                .filter(Property::isObjectProperty)
                .filter(Predicate.not(Property::hasVocabValue))
                .map(x -> value instanceof Literal
                        ? (((Literal) value).isWildcard() ? null : JsonLd.SEARCH_KEY)
                        : JsonLd.ID_KEY);
    }

    private static Map<String, Object> equalsFilter(String path, String value) {
        return equalsFilter(path, value, false);
    }

    private static Map<String, Object> notEqualsFilter(String path, String value) {
        return equalsFilter(path, value, true);
    }

    private static Map<String, Object> equalsFilter(String path, String value, boolean negate) {
        var clause = new HashMap<>();
        boolean isSimple = ESQuery.isSimple(value);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        var sq = new HashMap<>();
        sq.put("query", isSimple ? value : ESQuery.escapeNonSimpleQueryString(value));
        sq.put("fields", new ArrayList<>(List.of(path)));
        sq.put("default_operator", "AND");
        clause.put(queryMode, sq);
        return negate ? filterWrap(mustNotWrap(clause)) : filterWrap(clause);
    }

    private static Map<String, Object> rangeFilter(String path, String value, String key) {
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
}
package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.search2.QueryParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.search2.Operator.EQUALS;
import static whelk.search2.Operator.GREATER_THAN;
import static whelk.search2.Operator.NOT_EQUALS;
import static whelk.search2.QueryUtil.boolWrap;
import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.nestedWrap;
import static whelk.search2.QueryUtil.parenthesize;

public record PathValue(Path path, Operator operator, Value value) implements Node {
    public PathValue(Property property, Operator operator, Value value) {
        this(new Path(property), operator, value);
    }

    public PathValue(String key, Operator operator, Value value) {
        this(new Path.ExpandedPath(new Key.RecognizedKey(key)), operator, value);
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        if (value instanceof FreeText ft) {
            // FIXME: This is only needed until frontend no longer rely on quoted values not being treated as such.
            List<Token> unquotedTokens = ft.tokens().stream()
                    .map(t -> t.isQuoted() ? new Token.Raw(t.value(), t.offset()) : t)
                    .toList();
            FreeText newFt = new FreeText(ft.textQuery(), ft.negate(), unquotedTokens, ft.connective());
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
        return operator.format(path.queryForm(), value.isMultiToken() ? parenthesize(value.queryForm()) : value.queryForm());
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public Node getInverse() {
        return new PathValue(path, operator.getInverse(), value);
    }

    @Override
    public boolean isTypeNode() {
        return (getSoleProperty().filter(Property::isRdfType).isPresent() || getSoleKey().filter(Key::isType).isPresent())
                && operator.equals(EQUALS)
                && value instanceof VocabTerm;
    }

    public boolean hasEqualProperty(Property property) {
        return getSoleProperty().filter(property::equals).isPresent();
    }

    public boolean hasEqualProperty(String propertyKey) {
        return getSoleProperty().map(Property::name).filter(propertyKey::equals).isPresent();
    }

    public PathValue withOperator(Operator replacement) {
        return new PathValue(path, replacement, value);
    }

    public PathValue withValue(Value replacement) {
        return new PathValue(path, operator, replacement);
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
                .map(nestedStem -> operator == NOT_EQUALS
                        ? mustNotWrap(nestedWrap(nestedStem, withOperator(EQUALS).getCoreEsQuery(esSettings)))
                        : mustWrap(nestedWrap(nestedStem, getCoreEsQuery(esSettings))));
    }

    private Map<String, Object> _getCoreEsQuery(ESSettings esSettings) {
        String p = path.jsonForm();
        Value v = value;

        EsMappings esMappings = esSettings.mappings();

        if ((v instanceof Numeric n && !esMappings.isFourDigitField(p) && !esMappings.isLongField(p))) {
            // Treat as free text
            v = new FreeText(n.toString());
        } else if (v instanceof DateTime d && !esMappings.isDateField(p)) {
            // TODO: What about e.g. :firstIssueDate/:lastIssueDate? These have range xsd:date however are not indexed as date type in ES.
            v = new FreeText(d.toString());
        }

        Function<Object, Map<String, Object>> numOrDateFilter = numOrDate -> switch (operator) {
            case EQUALS -> filterWrap(buildTermQuery(p, numOrDate));
            case NOT_EQUALS -> mustNotWrap(buildTermQuery(p, numOrDate));
            case GREATER_THAN_OR_EQUALS -> esRangeFilter(p, numOrDate, "gte");
            case GREATER_THAN -> esRangeFilter(p, numOrDate, "gt");
            case LESS_THAN_OR_EQUALS -> esRangeFilter(p, numOrDate, "lte");
            case LESS_THAN -> esRangeFilter(p, numOrDate, "lt");
        };

        return switch (v) {
            case DateTime dateTime -> numOrDateFilter.apply(dateTime.dateTime().toElasticDateString());
            case FreeText ft -> {
                if (ft.isWild()) {
                    yield switch (operator) {
                        case EQUALS -> existsFilter(p);
                        case NOT_EQUALS -> notExistsFilter(p);
                        // FIXME: Range makes no sense here
                        default -> nonsenseFilter();
                    };
                }
                var boostSettings = esSettings.boost().fieldBoost();

                yield switch (operator) {
                    case EQUALS -> ft.toEs(boostSettings.withField(p));
                    case NOT_EQUALS -> mustNotWrap(ft.toEs(boostSettings.withField(p, 0)));
                    // FIXME: Range makes no sense here
                    default -> nonsenseFilter();
                };
            }
            case InvalidValue ignored -> nonsenseFilter(); // TODO: Treat whole expression as free text?
            case Numeric numeric -> numOrDateFilter.apply(numeric.value()); // TODO: Sort out keyword fields, e.g. what is indexed into publication.year.keyword
            case Resource resource -> switch (operator) {
                case EQUALS -> filterWrap(buildTermQuery(p, resource.jsonForm()));
                case NOT_EQUALS -> mustNotWrap(buildTermQuery(p, resource.jsonForm()));
                // FIXME: Range makes no sense here
                default -> nonsenseFilter();
            };
        };
    }

    private Map<String, Object> _toSearchMapping(QueryTree qt, QueryParams queryParams) {
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
        m.put("up", makeUpLink(qt, this, queryParams));

        m.put("_key", path.queryForm());
        m.put("_value", value.queryForm());

        return m;
    }

    private Node _expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        Path.ExpandedPath expandedPath = path.expand(jsonLd, value);

        if (!rulingTypes.isEmpty()) {
            List<Path.ExpandedPath> altPaths = expandedPath.getAltPaths(jsonLd, rulingTypes);
            List<Path.ExpandedPath> alt2Paths = expandedPath.getAlt2Paths(jsonLd);
            var altPvNodes = Stream.concat(altPaths.stream(), alt2Paths.stream())
                    .map(ap -> new PathValue(ap, operator, value).expand(jsonLd))
                    .toList();
            return altPaths.size() > 1
                    ? (operator == NOT_EQUALS ? new And(altPvNodes) : new Or(altPvNodes))
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
                .map(t -> (Node) new PathValue(path, operator, new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return operator == NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    private static Map<String, Object> esRangeFilter(String path, Object value, String key) {
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

    private static Map<String, Object> buildTermQuery(String field, Object value) {
        return Map.of("term", Map.of(field, value));
    }

    // FIXME: Handle queries that are syntactically correct but make no sense and are guaranteed to return no hits
    private static Map<String, Object> nonsenseFilter() {
        return existsFilter("nonsense.field");
    }
}
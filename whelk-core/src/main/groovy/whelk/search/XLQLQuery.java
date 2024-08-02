package whelk.search;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.util.DocumentUtil;
import whelk.util.Unicode;
import whelk.xlql.Ast;
import whelk.xlql.Disambiguate;
import whelk.xlql.FlattenedAst;
import whelk.xlql.Lex;
import whelk.xlql.Operator;
import whelk.xlql.Parse;
import whelk.xlql.Path;
import whelk.xlql.QueryTree;
import whelk.xlql.SimpleQueryTree;


import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static whelk.component.ElasticSearch.flattenedLangMapKey;
import static whelk.util.DocumentUtil.NOP;
import static whelk.xlql.Disambiguate.RDF_TYPE;
import static whelk.xlql.SimpleQueryTree.pvEqualsLink;

public class XLQLQuery {
    private final Whelk whelk;
    private final Disambiguate disambiguate;
    private final ESQueryLensBoost lensBoost;
    private Map<String, String> expandedPathToProperty = new HashMap<>();

    public final EsMappings esMappings;

    private static final String FILTERED_AGG_NAME = "a";
    private static final String NESTED_AGG_NAME = "n";
    private static final int DEFAULT_BUCKET_SIZE = 10;
    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    public record Entity(String id, String rdfType) {}

    public XLQLQuery(Whelk whelk) {
        this.whelk = whelk;
        this.disambiguate = new Disambiguate(whelk);
        this.lensBoost = new ESQueryLensBoost(whelk.getJsonld());
        this.esMappings = new EsMappings(whelk.elastic != null ? whelk.elastic.getMappings() : Collections.emptyMap());
    }

    public QueryTree getQueryTree(SimpleQueryTree sqt, Disambiguate.OutsetType outsetType) {
        return new QueryTree(sqt, disambiguate, outsetType, esMappings.nestedFields);
    }

    public SimpleQueryTree getSimpleQueryTree(String queryString, Map<String, SimpleQueryTree.Node> aliasedFilters) throws InvalidQueryException {
        if (queryString.isEmpty()) {
            return new SimpleQueryTree(null);
        }
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        Ast ast = new Ast(parseTree);
        FlattenedAst flattened = new FlattenedAst(ast);
        return new SimpleQueryTree(flattened, disambiguate, aliasedFilters);
    }

    public String sqtToQueryString(SimpleQueryTree sqt) {
        return sqt.toQueryString(disambiguate);
    }

    public SimpleQueryTree addDefaultFilters(SimpleQueryTree sqt, List<SimpleQueryTree.Node> filters) {
        var currentPvNodes = sqt.getTopLevelPvNodes();
        var currentBfNodes = sqt.getTopLevelBfNodes();
        var newTree = sqt;

        Function<SimpleQueryTree.PropertyValue, Boolean> isTypeEquals = pv ->
                pv.property().equals(RDF_TYPE) && pv.operator().equals(Operator.EQUALS);

        for (var node : filters) {
            switch (node) {
                case SimpleQueryTree.PropertyValue pv -> {
                    if (isTypeEquals.apply(pv)) {
                        if (currentPvNodes.stream().noneMatch(isTypeEquals::apply)) {
                            newTree = newTree.andExtend(pv);
                        }
                    } else {
                        newTree = newTree.andExtend(pv);
                    }
                }
                case SimpleQueryTree.BoolFilter bf -> {
                    if (currentBfNodes.stream().noneMatch(n -> n.getConcernedAliases().contains(bf.alias()))) {
                        newTree = newTree.andExtend(bf);
                    }
                }
                default -> {}
            }
        }

        return newTree;
    }

    public SimpleQueryTree normalizeFilters(SimpleQueryTree sqt, StatsRepr statsRepr) throws InvalidQueryException {
        // Map to alias if possible, e.g. "NOT excludeEplikt" -> "includeEplikt"
        for (var node : sqt.getTopLevelNodes()) {
            var matchingAliased = statsRepr.aliasedFilters.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().equals(node)) // TODO: Improve equality check
                    .findFirst();
            if (matchingAliased.isPresent()) {
                var aliasNode = getSimpleQueryTree(matchingAliased.get().getKey(), statsRepr.aliasedFilters).tree;
                sqt = sqt.removeTopLevelNode(node).andExtend(aliasNode);
            }
        }

        // Remove any default filter from the explicit query
        sqt = sqt.removeTopLevelNodes(statsRepr.siteDefaultFilters);

        // Remove meaningless negations, e.g. "NOT includeEplikt" if "includeEplikt" is an available filter
        var negatedAvailableFilters = sqt.getTopLevelNodes().stream()
                .filter(n -> n instanceof SimpleQueryTree.BoolFilter)
                .filter(n -> !((SimpleQueryTree.BoolFilter) n).isActive())
                .filter(n -> statsRepr.availableBoolFilters.stream()
                        .map(f -> f.get("filter"))
                        .filter(f -> f instanceof SimpleQueryTree.BoolFilter)
                        .anyMatch(f -> ((SimpleQueryTree.BoolFilter) f).alias().equals(((SimpleQueryTree.BoolFilter) n).alias())))
                .toList();
        sqt = sqt.removeTopLevelNodes(negatedAvailableFilters);

        return sqt;
    }

    public Map<String, Object> getEsQuery(QueryTree queryTree) {
        return buildEsQuery(queryTree.tree, new HashMap<>());
    }

    private Map<String, Object> buildEsQuery(QueryTree.Node qtNode, Map<String, Object> esQueryNode) {
        switch (qtNode) {
            case QueryTree.And and -> {
                var mustClause = and.conjuncts()
                        .stream()
                        .map(c -> buildEsQuery(c, new HashMap<>()))
                        .toList();
                esQueryNode.putAll(mustWrap(mustClause));
                return esQueryNode;
            }
            case QueryTree.Or or -> {
                var shouldClause = or.disjuncts()
                        .stream()
                        .map(d -> buildEsQuery(d, new HashMap<>()))
                        .toList();
                esQueryNode.putAll(shouldWrap(shouldClause));
                return esQueryNode;
            }
            case QueryTree.Nested n -> {
                return esNestedFilter(n);
            }
            case QueryTree.FreeText ft -> {
                return esFreeText(ft);
            }
            case QueryTree.Field f -> {
                return esFilter(f);
            }
        }
    }

    private Map<String, Object> esFreeText(QueryTree.FreeText ft) {
        String s = ft.value();
        s = Unicode.normalizeForSearch(s);
        boolean isSimple = ESQuery.isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = ESQuery.escapeNonSimpleQueryString(s);
        }
        Map<String, Object> simpleQuery = new HashMap<>();
        simpleQuery.put(queryMode,
                Map.of("query", s,
                        "analyze_wildcard", true,
                        "default_operator", "AND"
                )
        );

        // TODO: Boost by type
        List<String> boostedFields = lensBoost.computeBoostFieldsFromLenses(new String[0]);

        if (boostedFields.isEmpty()) {
            if (ft.operator() == Operator.EQUALS) {
                return simpleQuery;
            }
            if (ft.operator() == Operator.NOT_EQUALS) {
                return mustNotWrap(simpleQuery);
            }
        }

        List<String> softFields = boostedFields.stream()
                .filter(f -> f.contains(JsonLd.SEARCH_KEY))
                .toList();
        List<String> exactFields = boostedFields.stream()
                .map(f -> f.replace(JsonLd.SEARCH_KEY, JsonLd.SEARCH_KEY + ".exact"))
                .toList();

        var boostedExact = new HashMap<>();
        var be = Map.of(
                "query", s,
                "fields", exactFields,
                "analyze_wildcard", true,
                "default_operator", "AND"
        );
        boostedExact.put(queryMode, be);

        var boostedSoft = new HashMap<>();
        var bs = Map.of(
                "query", s,
                "fields", softFields,
                "quote_field_suffix", ".exact",
                "analyze_wildcard", true,
                "default_operator", "AND"
        );
        boostedSoft.put(queryMode, bs);

        var shouldClause = new ArrayList<>(Arrays.asList(boostedExact, boostedSoft, simpleQuery));
        if (ft.operator() == Operator.EQUALS) {
            return shouldWrap(shouldClause);
        }
        if (ft.operator() == Operator.NOT_EQUALS) {
            /*
            Better with { must: [must_not:{}, must_not:{}, must_not:{}] }?
            https://opster.com/guides/elasticsearch/search-apis/elasticsearch-query-bool/
            Limit the use of should clauses:
            While `should` clauses can be useful for boosting scores, they can also slow down your queries if used excessively.
            Try to limit the use of `should` clauses and only use them when necessary.
             */
            return mustNotWrap(shouldWrap(shouldClause));
        }
        throw new RuntimeException("Invalid operator"); // Not reachable
    }

    private Map<String, Object> esFilter(QueryTree.Field f) {
        String path = f.path();
        String value = f.value();

        if (Operator.WILDCARD.equals(value)) {
            return switch (f.operator()) {
                case EQUALS -> existsFilter(path);
                case NOT_EQUALS -> notExistsFilter(path);
                default -> notExistsFilter(path); // TODO?
            };
        }

        return switch (f.operator()) {
            case EQUALS -> equalsFilter(path, value);
            case NOT_EQUALS -> notEqualsFilter(path, value);
            case LESS_THAN -> rangeFilter(path, value, "lt");
            case LESS_THAN_OR_EQUALS -> rangeFilter(path, value, "lte");
            case GREATER_THAN -> rangeFilter(path, value, "gt");
            case GREATER_THAN_OR_EQUALS -> rangeFilter(path, value, "gte");
        };
    }

    private Map<String, Object> esNestedFilter(QueryTree.Nested n) {
        // TODO: Use simple_query_string / query_string instead of match?
        var musts = n.fields()
                .stream()
                .flatMap(f -> Arrays.stream(f.value().split("\\s+"))
                        .map(word -> Map.of("match", Map.of(f.path(), word))))
                .toList();

        Map<String, Object> nested = Map.of(
                "nested", Map.of(
                        "path", String.join(".", n.stem()),
                        "query", mustWrap(musts)
                )
        );

        return n.operator() == Operator.NOT_EQUALS ? mustNotWrap(nested) : mustWrap(nested);
    }

    public Map<String, Object> toMappings(SimpleQueryTree sqt) {
        return toMappings(sqt, Collections.emptyMap(), Collections.emptyList(), Collections.emptyList());
    }

    public Map<String, Object> toMappings(SimpleQueryTree sqt,
                                          Map<String, String> aliases,
                                          List<Map<String, Object>> filters,
                                          List<String> nonQueryParams) {
        return sqt.isEmpty()
                ? Collections.emptyMap()
                : buildMappings(sqt.tree, sqt, new LinkedHashMap<>(), aliases, filters, nonQueryParams);
    }

    private Map<String, Object> buildMappings(SimpleQueryTree.Node sqtNode,
                                              SimpleQueryTree sqt,
                                              Map<String, Object> mappingsNode,
                                              Map<String, String> aliases,
                                              List<Map<String, Object>> filters,
                                              List<String> nonQueryParams) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                var andClause = and.conjuncts()
                        .stream()
                        .map(c -> buildMappings(c, sqt, new LinkedHashMap<>(), aliases, filters, nonQueryParams))
                        .toList();
                mappingsNode.put("and", andClause);
            }
            case SimpleQueryTree.Or or -> {
                var orClause = or.disjuncts()
                        .stream()
                        .map(d -> buildMappings(d, sqt, new LinkedHashMap<>(), aliases, filters, nonQueryParams))
                        .toList();
                mappingsNode.put("or", orClause);
            }
            case SimpleQueryTree.FreeText ft -> mappingsNode = freeTextMapping(ft);
            case SimpleQueryTree.PropertyValue pv -> mappingsNode = propertyValueMapping(pv, aliases);
            case SimpleQueryTree.BoolFilter bf -> mappingsNode = boolFilterMapping(bf, filters);
        }

        SimpleQueryTree reducedTree = sqt.excludeFromTree(sqtNode);
        String upUrl = reducedTree.isEmpty() && !sqt.isFreeText() && !sqt.getFreeTextPart().isEmpty()
                ? makeFindUrl(sqt.getFreeTextPart(), sqt.getFreeTextPart(), nonQueryParams)
                : makeFindUrl(reducedTree, nonQueryParams);

        mappingsNode.put("up", Map.of(JsonLd.ID_KEY, upUrl));

        return mappingsNode;
    }

    private String makeFindUrl(SimpleQueryTree sqt, List<String> nonQueryParams) {
        return makeFindUrl(sqt.getFreeTextPart(), sqt.toQueryString(disambiguate), nonQueryParams);
    }

    private static String makeFindUrl(String i, String q, List<String> nonQueryParams) {
        List<String> params = new ArrayList<>();
        params.add(makeParam("_i", i));
        params.add(makeParam("_q", q));
        params.addAll(nonQueryParams);
        return makeFindUrl(params);
    }

    private static String makeFindUrl(List<String> params) {
        return "/find?" + String.join("&", params);
    }

    public static List<String> makeParams(Map<String, String> params) {
        return params.entrySet()
                .stream()
                .map(entry -> makeParam(entry.getKey(), entry.getValue()))
                .toList();
    }

    public static String makeParam(String key, String value) {
        return String.format("%s=%s", escapeQueryParam(key), escapeQueryParam(value));
    }

    private static String escapeQueryParam(String input) {
        return QUERY_ESCAPER.escape(input)
                // We want pretty URIs, restore some characters which are inside query strings
                // https://tools.ietf.org/html/rfc3986#section-3.4
                .replace("%3A", ":")
                .replace("%2F", "/")
                .replace("%40", "@");
    }

    public static String encodeUri(String uri) {
        String decoded = URLDecoder.decode(uri.replace("+", "%2B"), StandardCharsets.UTF_8);
        return escapeQueryParam(decoded)
                .replace("%23", "#")
                .replace("+", "%20");
    }

    private Map<String, Object> freeTextMapping(SimpleQueryTree.FreeText ft) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", getDefinition("textQuery"));
        m.put(ft.operator().termKey, ft.value());
        return m;
    }

    private Map<String, Object> boolFilterMapping(SimpleQueryTree.BoolFilter bf, List<Map<String, Object>> filters) {
        Map<String, Object> m = new LinkedHashMap<>();

        filters.stream().filter(f -> f.get("filter") instanceof SimpleQueryTree.BoolFilter)
                .filter(f -> bf.alias().equals(((SimpleQueryTree.BoolFilter) f.get("filter")).alias()))
                .map(f -> Map.of("prefLabelByLang", f.get("prefLabelByLang"), JsonLd.TYPE_KEY, "Resource"))
                .findFirst()
                .ifPresentOrElse(o -> m.put("object", o), () -> m.put("value", bf.alias()));

        return m;
    }

    private Map<String, Object> propertyValueMapping(SimpleQueryTree.PropertyValue pv, Map<String, String> aliases) {
        Map<String, Object> m = new LinkedHashMap<>();

        if (pv.path().size() > 1) {
            var propertyChainAxiom = pv.path().stream()
                    .map(this::getDefinition)
                    .filter(Objects::nonNull)
                    .toList();
            var propDef = propertyChainAxiom.size() > 1
                    ? Map.of("propertyChainAxiom", propertyChainAxiom)
                    : getDefinition(pv.property());
            m.put("property", propDef);
        } else {
            m.put("property", getDefinition(pv.property()));
            Optional.ofNullable(aliases.get(pv.property())).ifPresent(a -> m.put("alias", a));
        }

        m.put(pv.operator().termKey, lookUp(pv.value()));

        return m;
    }

    private Object lookUp(SimpleQueryTree.Value value) {
        return switch (value) {
            case SimpleQueryTree.VocabTerm vocabTerm -> getDefinition(vocabTerm.string());
            case SimpleQueryTree.Link link -> disambiguate.loadThing(value.string(), whelk)
                    .map(whelk.getJsonld()::toChip)
                    .orElse(link.string());
            case SimpleQueryTree.Literal literal -> literal.string();
        };
    }

    private Map<?, ?> getDefinition(String term) {
        return whelk.getJsonld().vocabIndex.get(term);
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

    private static Map<String, Object> mustWrap(Object l) {
        return boolWrap(Map.of("must", l));
    }

    private static Map<String, Object> mustNotWrap(Object o) {
        return boolWrap(Map.of("must_not", o));
    }

    private static Map<String, Object> shouldWrap(List<?> l) {
        return boolWrap(Map.of("should", l));
    }

    private static Map<String, Object> boolWrap(Map<?, ?> m) {
        return Map.of("bool", m);
    }

    private static Map<String, Object> filterWrap(Map<?, ?> m) {
        return boolWrap(Map.of("filter", m));
    }

    private static Map<String, Object> rangeWrap(Map<?, ?> m) {
        return Map.of("range", m);
    }

    private static String quoteIfPhrase(String s) {
        return s.matches(".*\\s.*") ? "\"" + s + "\"" : s;
    }

    public static String quoteIfPhraseOrContainsSpecialSymbol(String s) {
        return s.matches(".*(>=|<=|[=!~<>(): ]).*") ? "\"" + s + "\"" : s;
    }

    public Map<String, Object> pAgg(Entity object, Disambiguate.OutsetType outsetType) {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = curatedPredicates(object)
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        p -> getEsQuery(new QueryTree(new SimpleQueryTree(pvEqualsLink(p, object.id)), disambiguate, outsetType, esMappings.nestedFields))));

        if (!filters.isEmpty()) {
            query.put("_p", Map.of("filters", Map.of("filters", filters)));
        }

        return query;
    }

    private List<String> curatedPredicates(Entity object) {
        // TODO don't hardcode
        Map<String, List<String>> typeToRelations =
                Map.of(
                        "Agent", List.of("contributor", "subject", "publisher"),
                        "Concept", List.of("subject", "genreForm", "hasOccupation", "fieldOfActivity"),
                        "Work", List.of("subject")
                );

        var types = new ArrayList<String>();
        types.add(object.rdfType);
        whelk.getJsonld().getSuperClasses(object.rdfType, types);
        return types.stream()
                .filter(typeToRelations::containsKey)
                .findFirst().map(typeToRelations::get)
                .orElse(Collections.emptyList());
    }

    public Map<String, Object> getAggQuery(Map<?, ?> statsRepr, Disambiguate.OutsetType outsetType) {
        if (statsRepr.isEmpty()) {
            return Map.of(JsonLd.TYPE_KEY,
                    Map.of("terms",
                            Map.of("field", JsonLd.TYPE_KEY)));
        }
        return buildAggQuery(statsRepr, outsetType);
    }

    private Map<String, Object> buildAggQuery(Map<?, ?> statsRepr, Disambiguate.OutsetType outsetType) {
        Map<String, Object> query = new LinkedHashMap<>();

        for (var entry : statsRepr.entrySet()) {
            var property = (String) entry.getKey();
            var value = (Map<?, ?>) entry.getValue();

            String sort = "key".equals(value.get("sort")) ? "_key" : "_count";
            String sortOrder = "asc".equals(value.get("sort")) ? "asc" : "desc";

            int size = Optional.ofNullable((Integer) value.get("size")).orElse(DEFAULT_BUCKET_SIZE);

            Path path = new Path(List.of(property));
            path.expand(property, disambiguate, outsetType);

            for (Path stem : path.getAltStems()) {
                var altPaths = path.branches.stream().map(stem::attachBranch).toList();
                var p = switch (altPaths.size()) {
                    case 0 -> stem.stringify();
                    case 1 -> altPaths.getFirst().stringify();
                    // TODO: E.g. author (combining contribution.role and contribution.agent)
                    default -> throw new RuntimeException("Can't handle combined fields in aggs query");
                };

                // Core agg query
                var aggs = Map.of("terms",
                        Map.of("field", p,
                                "size", size,
                                "order", Map.of(sort, sortOrder)));

                // If field is nested, wrap agg query with nested
                var nested = getNestedPath(p);
                if (nested.isPresent()) {
                    aggs = Map.of("nested", Map.of("path", nested.get()),
                            "aggs", Map.of(NESTED_AGG_NAME, aggs));
                }

                // Wrap agg query with a filter
                var filter = mustWrap(Collections.emptyList());
                aggs = Map.of("aggs", Map.of(FILTERED_AGG_NAME, aggs),
                        "filter", filter);

                query.put(p, aggs);
                expandedPathToProperty.put(p, property);
            }
        }

        return query;
    }

    private Optional<String> getNestedPath(String path) {
        if (esMappings.isNestedField(path)) {
            return Optional.of(path);
        }
        return esMappings.nestedFields.stream().filter(path::startsWith).findFirst();
    }

    public Map<String, Object> getStats(Map<String, Object> esResponse, StatsRepr statsRepr, SimpleQueryTree sqt, Map<String, String> nonQueryParams, Map<String, String> aliases) {
        var sliceByDimension = getSliceByDimension(esResponse, statsRepr.statsRepr, sqt, nonQueryParams, aliases);
        var boolFilters = getBoolFilters(sqt, statsRepr.availableBoolFilters, nonQueryParams);
        return Map.of(JsonLd.ID_KEY, "#stats",
                "sliceByDimension", sliceByDimension,
                "_boolFilters", boolFilters);
    }

    private Map<String, Object> getSliceByDimension(Map<String, Object> esResponse, Map<String, Object> statsRepr, SimpleQueryTree sqt, Map<String, String> nonQueryParams, Map<String, String> aliases) {
        var buckets = collectBuckets(esResponse, statsRepr);
        var rangeProps = getRangeProperties(statsRepr);
        return buildSliceByDimension(buckets, sqt, nonQueryParams, aliases, rangeProps);
    }

    // Problem: Same value in different fields will be counted twice, e.g. contribution.agent + instanceOf.contribution.agent
    private Map<String, Map<SimpleQueryTree.PropertyValue, Integer>> collectBuckets(Map<String, Object> esResponse, Map<String, Object> statsRepr) {
        if (!esResponse.containsKey("aggregations")) {
            return Collections.emptyMap();
        }

        var aggs = (Map<?, ?>) esResponse.get("aggregations");

        Map<String, Map<SimpleQueryTree.PropertyValue, Integer>> propertyToBuckets = new LinkedHashMap<>();
        Map<String, Integer> propertyToMaxBuckets = new HashMap<>();

        for (var entry : aggs.entrySet()) {
            var path = (String) entry.getKey();
            var m = (Map<?, ?>) entry.getValue();

            var agg = (Map<?, ?>) m.get(FILTERED_AGG_NAME);
            if (agg == null) {
                continue;
            }

            if (agg.containsKey(NESTED_AGG_NAME)) {
                agg = (Map<?, ?>) agg.get(NESTED_AGG_NAME);
            }

            var property = expandedPathToProperty.get(path);
            boolean isLinked = path.endsWith(JsonLd.ID_KEY);

            ((List<?>) agg.get("buckets"))
                    .stream()
                    .map(Map.class::cast)
                    .forEach(b -> {
                        var value = (String) b.get("key");
                        var count = (Integer) b.get("doc_count");
                        SimpleQueryTree.PropertyValue pv;
                        if (isLinked) {
                            pv = pvEqualsLink(property, value);
                        } else if (disambiguate.hasVocabValue(property)) {
                            pv = SimpleQueryTree.pvEqualsVocabTerm(property, value);
                        } else {
                            pv = SimpleQueryTree.pvEqualsLiteral(property, value);
                        }
                        var buckets = propertyToBuckets.computeIfAbsent(property, x -> new HashMap<>());
                        buckets.compute(pv, (k, v) -> (v == null) ? count : v + count);
                    });

            int bucketSize = Optional.ofNullable((Integer) ((Map<?, ?>) statsRepr.get(property)).get("size"))
                    .orElse(DEFAULT_BUCKET_SIZE);

            propertyToMaxBuckets.put(property, bucketSize);
        }

        for (String property : statsRepr.keySet()) {
            var buckets = propertyToBuckets.remove(property);
            if (buckets != null) {
                int maxBuckets = propertyToMaxBuckets.get(property);
                Map<SimpleQueryTree.PropertyValue, Integer> newBuckets = new LinkedHashMap<>();
                buckets.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(Math.min(maxBuckets, buckets.size()))
                        .forEach(entry -> newBuckets.put(entry.getKey(), entry.getValue()));
                propertyToBuckets.put(property, newBuckets);
            }
        }

        return propertyToBuckets;
    }

    private Map<String, Object> buildSliceByDimension(
            Map<String, Map<SimpleQueryTree.PropertyValue, Integer>> propToBuckets,
            SimpleQueryTree sqt,
            Map<String, String> nonQueryParams,
            Map<String, String> aliases,
            Set<String> rangeProps) {
        Map<String, Object> sliceByDimension = new LinkedHashMap<>();

        propToBuckets.forEach((property, buckets) -> {
            var sliceNode = new LinkedHashMap<>();
            var isRange = rangeProps.contains(property);
            var observations = getObservations(buckets, isRange ? sqt.removeTopLevelPvRangeNodes(property) : sqt, nonQueryParams);
            if (!observations.isEmpty()) {
                if (isRange) {
                    sliceNode.put("search", getRangeTemplate(property, sqt, makeParams(nonQueryParams)));
                }
                sliceNode.put("dimension", property);
                sliceNode.put("observation", observations);
                Optional.ofNullable(aliases.get(property)).ifPresent(a -> sliceNode.put("alias", a));
                sliceByDimension.put(property, sliceNode);
            }
        });

        return sliceByDimension;
    }

    private List<Map<String,Object>> getBoolFilters(SimpleQueryTree sqt, List<Map<String, Object>> availableBoolFilters, Map<String, String> nonQueryParams) {
        var existing = sqt.getTopLevelNodes();
        List<Map<String, Object>> results = new ArrayList<>();

        for (var bf : availableBoolFilters) {
            SimpleQueryTree.Node filterNode = (SimpleQueryTree.Node) bf.get("filter");

            SimpleQueryTree newTree;
            boolean isSelected;

            if (existing.contains(filterNode)) {
                newTree = sqt.removeTopLevelNode(filterNode);
                isSelected = true;
            } else {
                newTree = sqt.andExtend(filterNode);
                isSelected = false;
            }

            Map<String, Object> res = new LinkedHashMap<>();
            // TODO: fix form
            res.put("totalItems", 0);
            res.put("object", Map.of(JsonLd.TYPE_KEY, "Resource", "prefLabelByLang", bf.get("prefLabelByLang")));
            res.put("view", Map.of(JsonLd.ID_KEY, makeFindUrl(newTree, makeParams(nonQueryParams))));
            res.put("_selected", isSelected);

            results.add(res);
        }

        return results;
    }

    private static Set<String> getRangeProperties(Map<String, Object> statsRepr) {
        return statsRepr.entrySet()
                .stream()
                .filter(e -> ((Map<?, ?>) e.getValue()).containsKey("range"))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private List<Map<String, Object>> getObservations(Map<SimpleQueryTree.PropertyValue, Integer> buckets, SimpleQueryTree sqt, Map<String, String> nonQueryParams) {
        List<Map<String, Object>> observations = new ArrayList<>();

        buckets.forEach((pv, count) -> {
            Map<String, Object> observation = new LinkedHashMap<>();
            boolean queried = sqt.getTopLevelPvNodes().contains(pv) || pv.value().string().equals(nonQueryParams.get("_o"));
            if (!queried) {
                observation.put("totalItems", count);
                var url = makeFindUrl(sqt.andExtend(pv), makeParams(nonQueryParams));
                observation.put("view", Map.of(JsonLd.ID_KEY, url));
                observation.put("object", lookUp(pv.value()));
                observations.add(observation);
            }
        });

        return observations;
    }

    private Map<String, Object> getRangeTemplate(String property, SimpleQueryTree sqt, List<String> nonQueryParams) {
        var GtLtNodes = sqt.getTopLevelPvNodes().stream()
                .filter(pv -> pv.property().equals(property))
                .filter(pv -> switch (pv.operator()) {
                    case EQUALS, NOT_EQUALS -> false;
                    case GREATER_THAN_OR_EQUALS, GREATER_THAN, LESS_THAN_OR_EQUALS, LESS_THAN -> true;
                })
                .filter(pv -> pv.value().isNumeric())
                .toList();

        String min = null;
        String max = null;

        for (var pv : GtLtNodes) {
            var orEquals = pv.toOrEquals();
            if (orEquals.operator() == Operator.GREATER_THAN_OR_EQUALS && min == null) {
                min = orEquals.value().string();
            } else if (orEquals.operator() == Operator.LESS_THAN_OR_EQUALS && max == null) {
                max = orEquals.value().string();
            } else {
                // Not a proper range, reset and abort
                min = null;
                max = null;
                break;
            }
        }

        var tree = sqt.removeTopLevelPvNodesByOperator(property);

        Map<String, Object> template = new LinkedHashMap<>();

        var variable = disambiguate.getQueryCode(property).orElse(property);
        var placeholderNode = new SimpleQueryTree.FreeText(Operator.EQUALS, String.format("{?%s}", variable));
        var templateQueryString = tree.andExtend(placeholderNode).toQueryString(disambiguate);
        var templateUrl = makeFindUrl(tree.getFreeTextPart(), templateQueryString, nonQueryParams);
        template.put("template", templateUrl);

        var mapping = new LinkedHashMap<>();
        mapping.put("variable", variable);
        mapping.put(Operator.GREATER_THAN_OR_EQUALS.termKey, Objects.toString(min, ""));
        mapping.put(Operator.LESS_THAN_OR_EQUALS.termKey, Objects.toString(max, ""));
        template.put("mapping", mapping);

        return template;
    }

    public List<Map<?, ?>> predicateLinks (Map<String, Object> esResponse, Entity object, Map<String, String> nonQueryParams) {
        var result = new ArrayList<Map<?, ?>>();
        // FIXME Use constant for _p
        // whelk-core should not deal with query parameters at all? Should be handled in rest module?
        Set<String> selected = new HashSet<>();
        if (nonQueryParams.containsKey("_p")) {
            selected.add(nonQueryParams.get("_p"));
        }
        Map<String, ?> counts = (Map<String, ?>) DocumentUtil.getAtPath(esResponse, List.of("aggregations", "_p", "buckets"), Collections.emptyMap());

        for (String p : curatedPredicates(object)) {
            if (!counts.containsKey(p)) {
                continue;
            }

            int count = (int) ((Map) counts.get(p)).get("doc_count");

            if (count > 0) {
                Map<String, String> params = new HashMap<>(nonQueryParams);
                params.put("_p", p);
                result.add(Map.of(
                        "totalItems", count,
                        "view", Map.of(JsonLd.ID_KEY, makeFindUrl(makeParams(params))),
                        "object", lookUp(new SimpleQueryTree.VocabTerm(p)),
                        "_selected", selected.contains(p)
                ));
            }
        }

        return result;
    }

    public static Map<String, String> getAliasMappings(Disambiguate.OutsetType outsetType) {
        Map<String, String> m = new HashMap<>();

        switch (outsetType) {
            case INSTANCE -> {
                m.put(RDF_TYPE, "instanceType");
                m.put("instanceOfType", "workType");
            }
            case WORK, RESOURCE -> {
                m.put(RDF_TYPE, "workType");
                m.put("hasInstanceType", "instanceType");
            }
        }

        return m;
    }

    private String expandLangMapKeys(String field) {
        var parts = field.split("\\.");
        if (parts.length > 0) {
            assert whelk != null;
            var lastIx = parts.length - 1;
            if (whelk.getJsonld().langContainerAlias.containsKey(parts[lastIx])) {
                parts[lastIx] = flattenedLangMapKey(parts[lastIx]);
                return String.join(".", parts);
            }
        }
        return field;
    }

    public String getSortField(String termPath) {
        var path = expandLangMapKeys(termPath);
        if (esMappings.isKeywordField(path) && !esMappings.isFourDigitField(path)) {
            return String.format("%s.keyword", path);
        } else {
            return termPath;
        }
    }

    public Disambiguate.OutsetType getOutsetType(SimpleQueryTree sqt) {
        return disambiguate.decideOutset(sqt);
    }

    public class EsMappings {
        private final Set<String> keywordFields;
        private final Set<String> dateFields;
        private final Set<String> nestedFields;
        private final Set<String> nestedNotInParentFields;
        private final Set<String> numericExtractorFields;

        public EsMappings(Map<?, ?> mappings) {
            this.keywordFields = getKeywordFields(mappings);
            this.dateFields = getFieldsOfType("date", mappings);
            this.nestedFields = getFieldsOfType("nested", mappings);
            this.nestedNotInParentFields = new HashSet<>(nestedFields);
            this.nestedNotInParentFields.removeAll(getFieldsWithSetting("include_in_parent", true, mappings));
            this.numericExtractorFields = getFieldsWithAnalyzer("numeric_extractor", mappings);
        }

        public boolean isKeywordField(String fieldPath) {
            return keywordFields.contains(fieldPath);
        }

        public boolean isDateField(String fieldPath) {
            return dateFields.contains(fieldPath);
        }

        public boolean isNestedField(String fieldPath) {
            return nestedFields.contains(fieldPath);
        }

        public boolean isNestedNotInParentField(String fieldPath) {
            return nestedNotInParentFields.contains(fieldPath);
        }

        public boolean isFourDigitField(String fieldPath) {
            return numericExtractorFields.contains(fieldPath);
        }

        private static Set<String> getKeywordFields(Map<?, ?> mappings) {
            Predicate<Object> test = v -> v instanceof Map && ((Map<?, ?>) v).containsKey("keyword");
            return getFieldsWithSetting("fields", test, mappings);
        }

        private static Set<String> getFieldsOfType(String type, Map<?, ?> mappings) {
            return getFieldsWithSetting("type", type, mappings);
        }

        private static Set<String> getFieldsWithAnalyzer(String analyzer, Map<?, ?> mappings) {
            return getFieldsWithSetting("analyzer", analyzer, mappings);
        }

        private static Set<String> getFieldsWithSetting(String setting, Object value, Map<?, ?> mappings) {
            return getFieldsWithSetting(setting, value::equals, mappings);
        }

        private static Set<String> getFieldsWithSetting(String setting, Predicate<Object> cmp, Map<?, ?> mappings) {
            Set<String> fields = new HashSet<>();
            DocumentUtil.Visitor visitor = (v, path) -> {
                if (cmp.test(v)) {
                    var p = dropLast(path).stream()
                            .filter(s -> !"properties".equals(s))
                            .map(Object::toString)
                            .toList();
                    var field = String.join(".", p);
                    fields.add(field);
                }
                return NOP;
            };
            DocumentUtil.findKey(mappings.get("properties"), setting, visitor);
            return fields;
        }

        static <V> List<V> dropLast(List<V> list) {
            var l = new ArrayList<>(list);
            l.removeLast();
            return l;
        }
    }

    public class StatsRepr {
        public final Map<String, Object> statsRepr;
        public final List<SimpleQueryTree.Node> siteDefaultFilters;
        public final List<SimpleQueryTree.Node> siteDefaultTypeFilters;

        public final Map<String, SimpleQueryTree.Node> aliasedFilters;

        public final List<Map<String, Object>> availableBoolFilters;

        public StatsRepr(Map<String, Object> statsRepr) throws InvalidQueryException {
            this.statsRepr = statsRepr;
            this.aliasedFilters = getAliasedFilters();
            this.siteDefaultFilters = getSiteDefaultFilters();
            this.siteDefaultTypeFilters = getDefaultTypeFilters();
            this.availableBoolFilters = getAvailableBoolFilters();
        }

        private List<SimpleQueryTree.Node> getSiteDefaultFilters() throws InvalidQueryException {
            // TODO: get from apps.jsonld
            var defaultFilters = new ArrayList<SimpleQueryTree.Node>();

            for (String s : List.of("excludeEplikt", "excludePreliminary", "NOT inCollection:\"https://id.kb.se/term/uniformWorkTitle\"")) {
                defaultFilters.add(getSimpleQueryTree(s, aliasedFilters).tree);
            }

            return defaultFilters;
        }

        public List<SimpleQueryTree.Node> getDefaultTypeFilters() throws InvalidQueryException {
            // TODO: get from apps.jsonld
            var defaultFilters = new ArrayList<SimpleQueryTree.Node>();

            for (String s : List.of("\"rdf:type\":Work")) {
                defaultFilters.add(getSimpleQueryTree(s, aliasedFilters).tree);
            }

            return defaultFilters;
        }

        private Map<String, SimpleQueryTree.Node> getAliasedFilters() throws InvalidQueryException {
            // TODO: get from apps.jsonld
            Map<String, SimpleQueryTree.Node> aliasedFilters = new HashMap<>();

            for (Map<String, String> m : List.of(
                    Map.of("alias", "excludeEplikt", "filter", "NOT bibliography:\"sigel:EPLK\""),
                    Map.of("alias", "includeEplikt", "filter", "NOT excludeEplikt"),
                    Map.of("alias", "excludePreliminary", "filter", "NOT encodingLevel:(\"marc:PartialPreliminaryLevel\" OR \"marc:PrepublicationLevel\")"),
                    Map.of("alias", "includePreliminary", "filter", "NOT excludePreliminary")
            )) {
                aliasedFilters.put(m.get("alias"), getSimpleQueryTree(m.get("filter"), aliasedFilters).tree);
            }

            return aliasedFilters;
        }

        private List<Map<String, Object>> getAvailableBoolFilters() throws InvalidQueryException {
            // TODO: get from apps.jsonld
            return List.of(
                    Map.of("filter", getSimpleQueryTree("includeEplikt", aliasedFilters).tree,
                            "prefLabelByLang", Map.of("sv", "Inkludera e-plikt", "en", "Include electronic legal deposit")),
                    Map.of("filter", getSimpleQueryTree("includePreliminary", aliasedFilters).tree,
                            "prefLabelByLang", Map.of("sv", "Inkludera kommande publiceringar", "en", "Include upcoming publications")),
                    Map.of("filter", getSimpleQueryTree("image:*", aliasedFilters).tree,
                            "prefLabelByLang", Map.of("sv", "Resurser med omslags-/miniatyrbild", "en", "Only resources with cover/thumbnail"))
//                Map.of("filter", "_TODO", "selectHeader", Map.of("sv", "Endast material som finns online", "en", "Only material available online")),
//                Map.of("filter", "_TODO", "selectHeader", Map.of("sv", "Endast digitalt material", "en", "Digital material only"))
            );
        }
    }
}

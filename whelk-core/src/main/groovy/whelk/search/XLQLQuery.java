package whelk.search;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;

import whelk.util.Unicode;
import whelk.xlql.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static whelk.util.DocumentUtil.findKey;
import static whelk.util.DocumentUtil.NOP;

public class XLQLQuery {
    private final Whelk whelk;
    private final Disambiguate disambiguate;
    private final ESQueryLensBoost lensBoost;
    private final Set<String> esNestedFields;

    private Map<String, String> expandedPathToProperty = new HashMap<>();

    private static final String FILTERED_AGG = "a";
    private static final int DEFAULT_BUCKET_SIZE = 10;

    public XLQLQuery(Whelk whelk) {
        this.whelk = whelk;
        this.disambiguate = new Disambiguate(whelk);
        this.lensBoost = new ESQueryLensBoost(whelk.getJsonld());
        this.esNestedFields = getEsNestedFields(whelk);
    }

    public QueryTree getQueryTree(SimpleQueryTree sqt) {
        return new QueryTree(sqt, disambiguate);
    }

    public QueryTree getQueryTree(SimpleQueryTree sqt, Disambiguate.OutsetType outsetType) {
        return new QueryTree(sqt, disambiguate, outsetType);
    }

    public SimpleQueryTree getSimpleQueryTree(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        Ast ast = new Ast(parseTree);
        FlattenedAst flattened = new FlattenedAst(ast);
        return new SimpleQueryTree(flattened, disambiguate);
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
        String path = f.path().stringify();
        String value = quoteIfPhrase(f.value());
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
        var paths = n.fields()
                .stream()
                .map(f -> f.path().path)
                .toList();

        int shortestPath = paths.stream()
                .mapToInt(List::size)
                .min()
                .orElseThrow();

        List<String> stem = new ArrayList<>();
        for (int i = 0; i < shortestPath; i++) {
            int idx = i;
            var unique = paths.stream()
                    .map(p -> p.get(idx))
                    .collect(Collectors.toSet());
            if (unique.size() == 1) {
                stem.add(unique.iterator().next());
            }
        }

        if (stem.isEmpty() || !esNestedFields.contains(stem.getLast())) {
            // Treat as regular fields
            var clause = n.fields()
                    .stream()
                    .map(this::esFilter)
                    .toList();
            return n.operator() == Operator.NOT_EQUALS ? shouldWrap(clause) : mustWrap(clause);
        }

        var musts = n.fields()
                .stream()
                .map(f -> Map.of("match", Map.of(f.path().stringify(), f.value())))
                .toList();

        Map<String, Object> nested = Map.of(
                "nested", Map.of(
                        "path", String.join(".", stem),
                        "query", mustWrap(musts)
                )
        );

        return n.operator() == Operator.NOT_EQUALS ? mustNotWrap(nested) : mustWrap(nested);
    }

    public Map<String, Object> toMappings(SimpleQueryTree sqt) {
        return toMappings(sqt, Collections.emptyList());
    }

    public Map<String, Object> toMappings(SimpleQueryTree sqt, List<String> urlParams) {
        return buildMappings(sqt.tree, sqt, new LinkedHashMap<>(), urlParams);
    }

    private Map<String, Object> buildMappings(SimpleQueryTree.Node sqtNode, SimpleQueryTree sqt, Map<String, Object> mappingsNode, List<String> urlParams) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                var andClause = and.conjuncts()
                        .stream()
                        .map(c -> buildMappings(c, sqt, new LinkedHashMap<>(), urlParams))
                        .toList();
                mappingsNode.put("and", andClause);
            }
            case SimpleQueryTree.Or or -> {
                var orClause = or.disjuncts()
                        .stream()
                        .map(d -> buildMappings(d, sqt, new LinkedHashMap<>(), urlParams))
                        .toList();
                mappingsNode.put("or", orClause);
            }
            case SimpleQueryTree.FreeText ft -> mappingsNode = freeTextMapping(ft);
            case SimpleQueryTree.PropertyValue pv -> mappingsNode = propertyValueMapping(pv);
        }

        Optional<SimpleQueryTree.Node> reducedTree = getReducedTree(sqt, sqtNode);
        // TODO: Empty tree --> ???
        String upUrl = reducedTree.map(node -> "/find?_q=" + treeToQueryString(node)).orElse("/find?_q=*");
        upUrl += urlParams.stream()
                .map(p -> "&" + p)
                .collect(Collectors.joining(""));
        mappingsNode.put("up", Map.of(JsonLd.ID_KEY, upUrl));

        return mappingsNode;
    }

    private Map<String, Object> freeTextMapping(SimpleQueryTree.FreeText ft) {
        return mapping("textQuery", ft.value(), ft.operator(), Collections.emptyList());
    }

    private Map<String, Object> propertyValueMapping(SimpleQueryTree.PropertyValue pv) {
        return mapping(pv.property(), pv.value(), pv.operator(), pv.propertyPath());
    }

    private Map<String, Object> mapping(String property, String value, Operator operator, List<String> propertyPath) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (propertyPath.size() > 1) {
            var propertyChainAxiom = propertyPath.stream()
                    .map(this::getDefinition)
                    .filter(Objects::nonNull)
                    .toList();
            var propDef = propertyChainAxiom.size() > 1
                    ? Map.of("propertyChainAxiom", propertyChainAxiom)
                    : getDefinition(property);
            m.put("property", propDef);
        } else {
            m.put("property", getDefinition(property));
        }
        m.put(operator.termKey, lookUp(value).orElse(value));
        return m;
    }

    private Optional<Object> lookUp(String value) {
        Optional<Object> vocabTerm = Optional.ofNullable(getDefinition(value));
        if (vocabTerm.isPresent()) {
            return vocabTerm;
        }

        value = Disambiguate.expandPrefixed(value);
        if (JsonLd.looksLikeIri(value)) {
            return disambiguate.loadThing(value, whelk)
                    .map(whelk.getJsonld()::toChip);
        }

        return Optional.empty();
    }

    private Map<?, ?> getDefinition(String term) {
        return whelk.getJsonld().vocabIndex.get(term);
    }

    private Optional<SimpleQueryTree.Node> getReducedTree(SimpleQueryTree sqt, SimpleQueryTree.Node qtNode) {
        return Optional.ofNullable(SimpleQueryTree.excludeFromTree(qtNode, sqt.tree));
    }

    private String treeToQueryString(SimpleQueryTree.Node sqtNode) {
        return buildQueryString(sqtNode, true);
    }

    private String buildQueryString(SimpleQueryTree.Node sqtNode, boolean topLevel) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                String andClause = and.conjuncts()
                        .stream()
                        .map(this::buildQueryString)
                        .collect(Collectors.joining(" AND "));
                return topLevel ? andClause : "(" + andClause + ")";
            }
            case SimpleQueryTree.Or or -> {
                String orClause = or.disjuncts()
                        .stream()
                        .map(this::buildQueryString)
                        .collect(Collectors.joining(" OR "));
                return topLevel ? orClause : "(" + orClause + ")";
            }
            case SimpleQueryTree.FreeText ft -> {
                return freeTextToString(ft);
            }
            case SimpleQueryTree.PropertyValue pv -> {
                return propertyValueToString(pv);
            }
        }
    }

    private String buildQueryString(SimpleQueryTree.Node sqtNode) {
        return buildQueryString(sqtNode, false);
    }

    private String freeTextToString(SimpleQueryTree.FreeText ft) {
        String s = quoteSpecialSymbolsWithinFreeTextString(ft.value());
        if (ft.operator() == Operator.NOT_EQUALS) {
            s = "NOT " + s;
        }
        return s;
    }

    private String propertyValueToString(SimpleQueryTree.PropertyValue pv) {
        String sep = switch (pv.operator()) {
            case EQUALS, NOT_EQUALS -> ":";
            case GREATER_THAN_OR_EQUALS -> ">=";
            case GREATER_THAN -> ">";
            case LESS_THAN_OR_EQUALS -> "<=";
            case LESS_THAN -> "<";
        };

        String not = pv.operator() == Operator.NOT_EQUALS ? "NOT " : "";
        String path = String.join(".", pv.propertyPath());

        return not + quoteIfPhraseOrContainsSpecialSymbol(path) + sep + quoteIfPhraseOrContainsSpecialSymbol(pv.value());
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
        clause.put(queryMode, sq);
        return negate ? filterWrap(mustNotWrap(clause)) : filterWrap(clause);
    }

    private static Map<String, Object> rangeFilter(String path, String value, String key) {
        return filterWrap(rangeWrap(Map.of(path, Map.of(key, value))));
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

    private static String quoteIfPhraseOrContainsSpecialSymbol(String s) {
        return s.matches(".*(>=|<=|[=!~<>(): ]).*") ? "\"" + s + "\"" : s;
    }

    private static String quoteSpecialSymbolsWithinFreeTextString(String s) {
        Matcher quotedMatcher = Pattern.compile("\".*?\"").matcher(s);
        List<List<Integer>> quotedIntervals = new ArrayList<>();
        while (quotedMatcher.find()) {
            quotedIntervals.add(List.of(quotedMatcher.start(), quotedMatcher.end()));
        }

        List<Integer> insertQuoteAtIdx = new ArrayList<>();
        Matcher specialSymbolMatcher = Pattern.compile("[^ \"]*(>=|<=|[=!~<>():])[^ \"]*").matcher(s);
        while (specialSymbolMatcher.find()) {
            boolean isAlreadyQuoted = quotedIntervals.stream().anyMatch(interval ->
                    interval.getFirst() < specialSymbolMatcher.start() && interval.getLast() > specialSymbolMatcher.end()
            );
            if (!isAlreadyQuoted) {
                insertQuoteAtIdx.addFirst(specialSymbolMatcher.start());
                insertQuoteAtIdx.addFirst(specialSymbolMatcher.end());
            }
        }

        for (int i : insertQuoteAtIdx) {
            s = s.substring(0, i) + "\"" + s.substring(i);
        }

        return s;
    }

    private Set<String> getEsNestedFields(Whelk whelk) {
        if (whelk.elastic == null) {
            return Collections.emptySet();
        }

        Set<String> fields = new HashSet<>();

        findKey(whelk.elastic.getMappings().get("properties"), "type", (Object v, List<Object> path) -> {
            if ("nested".equals(v)) {
                String field = path.subList(0, path.size() - 1).stream()
                        .filter(Predicate.not("properties"::equals))
                        .map(String.class::cast)
                        .collect(Collectors.joining("."));
                fields.add(field);
            }
            return NOP;
        });

        return fields;
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

            Path path = new Path(List.of(property));

            if (disambiguate.isObjectProperty(property) && !"rdf:type".equals(property)) {
                path.appendId();
            }

            List<String> altPaths = path.expand(property, disambiguate, outsetType)
                    .stream()
                    .map(Path::stringify)
                    .toList();

            int size = Optional.ofNullable((Integer) value.get("size")).orElse(DEFAULT_BUCKET_SIZE);

            for (String p : altPaths) {
                var aggs = Map.of(FILTERED_AGG,
                        Map.of("terms",
                                Map.of("field", p,
                                        "size", size,
                                        "order", Map.of(sort, sortOrder))));
                var filter = mustWrap(Collections.emptyList());
                query.put(p, Map.of("aggs", aggs, "filter", filter));
                expandedPathToProperty.put(p, property);
            }
        }

        return query;
    }

    // Problem: Same value in different fields will be counted twice, e.g. contribution.agent + instanceOf.contribution.agent
    private Map<String, Map<String, Integer>> collectAggs(Map<String, Object> esResponse, Map<String, Object> statsRepr) {
        Map<String, Map<String, Integer>> newAggs = new LinkedHashMap<>();

        if (esResponse.containsKey("aggregations")) {
            var aggs = (Map<?, ?>) esResponse.get("aggregations");
            Map<String, Integer> propertyToBucketSize = new HashMap<>();

            for (var entry : aggs.entrySet()) {
                var path = (String) entry.getKey();
                var value = (Map<?, ?>) entry.getValue();

                var filteredAgg = (Map<?, ?>) value.get(FILTERED_AGG);
                if (filteredAgg == null) {
                    continue;
                }

                var property = expandedPathToProperty.get(path);

                if (newAggs.containsKey(property)) {
                    Map<String, Integer> buckets = newAggs.get(property);
                    ((List<?>) filteredAgg.get("buckets"))
                            .stream()
                            .map(Map.class::cast)
                            .forEach(b -> {
                                var val = (String) b.get("key");
                                var count = (Integer) b.get("doc_count");
                                if (buckets.containsKey(val)) {
                                    buckets.put(val, buckets.get(val) + count);
                                } else {
                                    buckets.put(val, count);
                                }
                            });
                } else {
                    Map<String, Integer> buckets = new LinkedHashMap<>();
                    ((List<?>) filteredAgg.get("buckets"))
                            .stream()
                            .map(Map.class::cast)
                            .forEach(b -> buckets.put((String) b.get("key"), (Integer) b.get("doc_count")));
                    newAggs.put(property, buckets);
                }

                int size = Optional.ofNullable((Integer) ((Map<?, ?>) statsRepr.get(property)).get("size"))
                        .orElse(DEFAULT_BUCKET_SIZE);
                propertyToBucketSize.put(property, size);
            }

            for (var entry : newAggs.entrySet()) {
                String property = entry.getKey();

                Map<String, Integer> newBuckets = new LinkedHashMap<>();

                List<Map.Entry<String, Integer>> sorted = entry.getValue()
                        .entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue())
                        .toList()
                        .reversed();

                int bucketSize = propertyToBucketSize.get(property);
                (bucketSize >= sorted.size() ? sorted : sorted.subList(0, bucketSize))
                        .forEach(b -> newBuckets.put(b.getKey(), b.getValue()));

                newAggs.put(property, newBuckets);
            }
        }

        return newAggs;
    }

    public Map<String, Object> getStats(Map<String, Object> esResponse, Map<String, Object> statsRepr, SimpleQueryTree sqt, List<String> urlParams) {
        var aggs = collectAggs(esResponse, statsRepr);
        return buildStats(aggs, sqt, urlParams);
    }

    private Map<String, Object> buildStats(Map<String, Map<String, Integer>> aggs, SimpleQueryTree sqt, List<String> urlParams) {
        var sliceByDimension = new LinkedHashMap<>();

        aggs.forEach((property, buckets) -> {
            var sliceNode = new LinkedHashMap<>();
            var observations = getObservations(property, buckets, sqt, urlParams);
            if (!observations.isEmpty()) {
                // TODO: dimension/dimensionChain redundant
                //  Add property definition here to provide labels?
                sliceNode.put("dimension", property);
                sliceNode.put("dimensionChain", List.of(property));
                sliceNode.put("observation", observations);
                sliceByDimension.put(property, sliceNode);
            }
        });

        return Map.of(JsonLd.ID_KEY, "#stats",
                "sliceByDimension", sliceByDimension);
    }

    private List<Map<String, Object>> getObservations(String property, Map<String, Integer> buckets, SimpleQueryTree sqt, List<String> urlParams) {
        List<Map<String, Object>> observations = new ArrayList<>();

        String urlTail = urlParams.stream()
                .map(p -> "&" + p)
                .collect(Collectors.joining(""));

        buckets.forEach((value, count) -> {
            Map<String, Object> observation = new LinkedHashMap<>();
            SimpleQueryTree.PropertyValue pvNode = new SimpleQueryTree.PropertyValue(property, List.of(property), Operator.EQUALS, value);
            boolean queried = sqt.getTopLevelPvNodes().contains(pvNode);
            if (!queried) {
                observation.put("totalItems", count);
                String url = "/find?_q=" + treeToQueryString(new SimpleQueryTree.And(List.of(sqt.tree, pvNode))) + urlTail;
                observation.put("view", Map.of(JsonLd.ID_KEY, url));
                observation.put("object", lookUp(value).orElse(value));
                observations.add(observation);
            }
        });

        return observations;
    }

    public Disambiguate.OutsetType getOutsetType(SimpleQueryTree sqt) {
        return disambiguate.decideOutset(sqt);
    }
}

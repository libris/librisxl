package whelk.search;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.util.Unicode;
import whelk.xlql.*;

import java.util.*;
import java.util.stream.Collectors;

public class XLQLQuery {
    private Whelk whelk;
    private Disambiguate disambiguate;
    private ESQueryLensBoost lensBoost;

    public XLQLQuery(Whelk whelk) {
        this.whelk = whelk;
        this.disambiguate = new Disambiguate(whelk);
        this.lensBoost = new ESQueryLensBoost(whelk.getJsonld());
    }

    public QueryTree getQueryTree(SimpleQueryTree sqt) {
        return new QueryTree(sqt, disambiguate);
    }

    public SimpleQueryTree getSimpleQueryTree(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        Ast ast = new Ast(parseTree);
        FlattenedAst flattened = new FlattenedAst(ast);
        return new SimpleQueryTree(flattened, disambiguate);
    }

    // TODO: Unit tests
    public Map getEsQuery(QueryTree queryTree) {
        return buildEsQuery(queryTree.tree, new HashMap<>());
    }

    private Map buildEsQuery(QueryTree.Node qtNode, Map esQueryNode) {
        switch (qtNode) {
            case QueryTree.And and -> {
                List<Map> mustClause = and.conjuncts()
                        .stream()
                        .map(c -> buildEsQuery(c, new HashMap<>()))
                        .toList();
                esQueryNode.putAll(mustWrap(mustClause));
                return esQueryNode;
            }
            case QueryTree.Or or -> {
                List<Map> shouldClause = or.disjuncts()
                        .stream()
                        .map(d -> buildEsQuery(d, new HashMap<>()))
                        .toList();
                esQueryNode.putAll(shouldWrap(shouldClause));
                return esQueryNode;
            }

            case QueryTree.FreeText ft -> {
                return esFreeText(ft);
            }
            case QueryTree.Field f -> {
                return esFilter(f);
            }
        }
    }

    private Map esFreeText(QueryTree.FreeText ft) {
        String s = ft.value();
        s = Unicode.normalizeForSearch(s);
        s = quoteIfPhrase(s);
        boolean isSimple = ESQuery.isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = ESQuery.escapeNonSimpleQueryString(s);
        }
        Map<String, Map> simpleQuery = new HashMap<>();
        Map sq = simpleQuery.computeIfAbsent(queryMode, v -> new HashMap<>());
        sq.put("query", s);
        sq.put("analyze_wildcard", true);

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

        Map<String, Map> boostedExact = new HashMap<>();
        Map be = boostedExact.computeIfAbsent(queryMode, v -> new HashMap<>());
        be.put("query", s);
        be.put("fields", exactFields);
        be.put("analyze_wildcard", true);

        Map<String, Map> boostedSoft = new HashMap<>();
        Map bs = boostedSoft.computeIfAbsent(queryMode, v -> new HashMap<>());
        bs.put("query", s);
        bs.put("fields", softFields);
        bs.put("quote_field_suffix", ".exact");
        bs.put("analyze_wildcard", true);

        List<Map> shouldClause = new ArrayList<>(Arrays.asList(boostedExact, boostedSoft, simpleQuery));
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

    private Map esFilter(QueryTree.Field f) {
        String path = f.path().stringify();
        String value = quoteIfPhrase(f.value());
        return switch (f.operator()) {
            case EQUALS -> equalsFilter(path, value);
            case NOT_EQUALS -> notEqualsFilter(path, value);
            case LESS_THAN -> rangeFilter(path, value, "lt");
            case LESS_THAN_OR_EQUAL -> rangeFilter(path, value, "lte");
            case GREATER_THAN -> rangeFilter(path, value, "gt");
            case GREATER_THAN_OR_EQUAL -> rangeFilter(path, value, "gte");
        };
    }

    public Map toMappings(SimpleQueryTree sqt) {
        return toMappings(sqt, Collections.emptyList());
    }

    public Map toMappings(SimpleQueryTree sqt, List<String> urlParams) {
        return buildMappings(sqt.tree, sqt, new LinkedHashMap(), urlParams);
    }

    private Map buildMappings(SimpleQueryTree.Node sqtNode, SimpleQueryTree sqt, Map mappingsNode, List<String> urlParams) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                List<Map> andClause = and.conjuncts()
                        .stream()
                        .map(c -> buildMappings(c, sqt, new LinkedHashMap(), urlParams))
                        .toList();
                mappingsNode.put("and", andClause);
            }
            case SimpleQueryTree.Or or -> {
                List<Map> orClause = or.disjuncts()
                        .stream()
                        .map(d -> buildMappings(d, sqt, new LinkedHashMap(), urlParams))
                        .toList();
                mappingsNode.put("or", orClause);
            }
            case SimpleQueryTree.FreeText ft -> {
                mappingsNode = freeTextMapping(ft);
            }
            case SimpleQueryTree.PropertyValue pv -> {
                mappingsNode = propertyValueMapping(pv);
            }
        }

        Optional<SimpleQueryTree.Node> reducedTree = getReducedTree(sqt, sqtNode);
        // TODO: Empty tree --> ???
        String upUrl = reducedTree.isPresent() ? "/find?_q=" + treeToQueryString(reducedTree.get()) : "/find?_q=*";
        upUrl += urlParams.stream()
                .map(p -> "&" + p)
                .collect(Collectors.joining(""));
        mappingsNode.put("up", upUrl);

        return mappingsNode;
    }

    private Map mapping(String property, String value, Operator operator) {
        return mapping(property, value, operator, Collections.emptyList());
    }

    private Map mapping(String property, String value, Operator operator, List<String> propertyPath) {
        Map m = new LinkedHashMap();
        if (propertyPath.size() > 1) {
            m.put("variable", String.join(".", propertyPath));
            // Include "@id" / "_str" in chainAxiom?
            List propertyChainAxiom = propertyPath.stream()
                    .map(this::getDefinition)
                    .filter(Objects::nonNull)
                    .toList();
            Map predicate = propertyChainAxiom.size() > 1
                    ? Map.of("propertyChainAxiom", propertyChainAxiom)
                    : getDefinition(property);
            m.put("predicate", predicate);
        } else {
            m.put("variable", property);
            m.put("predicate", getDefinition(property));
        }
        m.put("value", value);
        m.put("operator", operator.termKey);
        return m;
    }

    private Map getDefinition(String property) {
        return whelk.getJsonld().vocabIndex.get(property);
    }

    private Map freeTextMapping(SimpleQueryTree.FreeText ft) {
        return mapping("textQuery", ft.value(), ft.operator());
    }

    private Map propertyValueMapping(SimpleQueryTree.PropertyValue pv) {
        return mapping(pv.property(), pv.value(), pv.operator(), pv.propertyPath());
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
        return ft.operator() == Operator.NOT_EQUALS ? "NOT " + ft.value() : quoteIfPhraseOrColon(ft.value());
    }

    private String propertyValueToString(SimpleQueryTree.PropertyValue pv) {
        String sep = switch (pv.operator()) {
            case EQUALS -> ": ";
            case NOT_EQUALS -> ": ";
            case GREATER_THAN_OR_EQUAL -> " >= ";
            case GREATER_THAN -> " > ";
            case LESS_THAN_OR_EQUAL -> " <= ";
            case LESS_THAN -> " < ";
        };
        String not = pv.operator() == Operator.NOT_EQUALS ? "NOT " : "";
        return not + String.join(".", pv.propertyPath()) + sep + quoteIfPhraseOrColon(pv.value());
    }

    private static Map equalsFilter(String path, String value) {
        return equalsFilter(path, value, false);
    }

    private static Map notEqualsFilter(String path, String value) {
        return equalsFilter(path, value, true);
    }

    private static Map equalsFilter(String path, String value, boolean negate) {
        Map<String, Map> clause = new HashMap<>();
        boolean isSimple = ESQuery.isSimple(value);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        Map sq = clause.computeIfAbsent(queryMode, v -> new HashMap<>());
        sq.put("query", isSimple ? value : ESQuery.escapeNonSimpleQueryString(value));
        sq.put("fields", new ArrayList<>(List.of(path)));
        return negate ? filterWrap(mustNotWrap(clause)) : filterWrap(clause);
    }

    private static Map rangeFilter(String path, String value, String key) {
        return filterWrap(rangeWrap(Map.of(path, Map.of(key, value))));
    }

    private static Map mustWrap(List l) {
        return boolWrap(Map.of("must", l));
    }

    private static Map mustNotWrap(Object o) {
        return boolWrap(Map.of("must_not", o));
    }

    private static Map shouldWrap(List l) {
        return boolWrap(Map.of("should", l));
    }

    private static Map boolWrap(Map m) {
        return Map.of("bool", m);
    }

    private static Map filterWrap(Map m) {
        return boolWrap(Map.of("filter", m));
    }

    private static Map rangeWrap(Map m) {
        return Map.of("range", m);
    }

    private static String quoteIfPhrase(String s) {
        return s.matches(".*\\s.*") ? "\"" + s + "\"" : s;
    }

    private static String quoteIfPhraseOrColon(String s) { return s.matches(".*[\s:].*") ? "\"" + s + "\"" : s; }
}

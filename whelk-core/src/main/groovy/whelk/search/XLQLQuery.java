package whelk.search;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.util.Unicode;
import whelk.xlql.BadQueryException;
import whelk.xlql.QueryTree;

import java.util.*;

public class XLQLQuery
{
    private Whelk whelk;
    private ESQuery esQuery;
    private QueryTree queryTree;

    XLQLQuery(Whelk whelk) {
        this.whelk = whelk;
        this.esQuery = new ESQuery(whelk);
        this.queryTree = new QueryTree(whelk);
    }

    public Map doQuery(Map<String, String[]> queryParameters) throws BadQueryException {
        if (!queryParameters.containsKey("_q")) {
            throw new RuntimeException("Missing _q parameter");
        }
        Object queryTree = this.queryTree.toQueryTree(getQueryString(queryParameters));
        Map query = toEsQuery(queryTree);
        // TODO
        // This is a shortcut to a complete ES query. Only temporary.
        Map ogEsQuery = this.esQuery.getESQuery(queryParameters);
        ogEsQuery.put("query", query);
        Map esResponse = esQuery.hideKeywordFields(esQuery.moveFilteredAggregationsToTopLevel(this.whelk.elastic.query(ogEsQuery)));
        if (Arrays.asList(queryParameters.get("_debug")).contains("esQuery")) {
            esResponse.put("_debug", newParent("esQuery", ogEsQuery));
        }
        return esResponse;
    }

    // TODO: Unit tests
    public Map toEsQuery(Object queryTree) {
        if (queryTree instanceof QueryTree.FreeText) {
            return esFreeText((QueryTree.FreeText) queryTree);
        }
        if (queryTree instanceof QueryTree.Field) {
            return esFilter((QueryTree.Field) queryTree);
        }
        return buildEsQuery(queryTree, new HashMap<>());
    }
    private Map buildEsQuery(Object qtNode, Map esQueryNode) {
        if (qtNode instanceof QueryTree.And) {
            List<Object> must = new ArrayList<>();
            for (Object c : ((QueryTree.And) qtNode).conjuncts()) {
                if (c instanceof QueryTree.FreeText) {
                    must.add(esFreeText((QueryTree.FreeText) c));
                } else if (c instanceof QueryTree.Field) {
                    must.add(esFilter((QueryTree.Field) c));
                } else {
                    must.add(buildEsQuery(c, new HashMap<>()));
                }
            }
            esQueryNode.putAll(mustWrap(must));
        } else if (qtNode instanceof QueryTree.Or) {
            List should = new ArrayList<>();
            for (Object d : ((QueryTree.Or) qtNode).disjuncts()) {
                if (d instanceof QueryTree.FreeText) {
                    should.add(esFreeText((QueryTree.FreeText) d));
                } else if (d instanceof QueryTree.Field) {
                    should.add(esFilter((QueryTree.Field) d));
                } else {
                    should.add(buildEsQuery(d, new HashMap<>()));
                }
            }
            esQueryNode.putAll(shouldWrap(should));
        }
        return esQueryNode;
    }

    private Map esFilter(QueryTree.Field f) {
        String path = f.path().stringify();
        String value = quoteIfPhrase(f.value());
        switch (f.operator()) {
            case EQUALS -> {
                return equalsFilter(path, value);
            }
            case NOT_EQUALS -> {
                return notEqualsFilter(path, value);
            }
            case LESS_THAN -> {
                return rangeFilter(path, value, "lt");
            }
            case LESS_THAN_OR_EQUAL -> {
                return rangeFilter(path, value, "lte");
            }
            case GREATER_THAN -> {
                return rangeFilter(path, value, "gt");
            }
            case GREATER_THAN_OR_EQUAL -> {
                return rangeFilter(path, value, "gte");
            }
            default -> {
                throw new RuntimeException("Unknown operator"); // Not reachable
            }
        }
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
        Map m = new HashMap<>();
        m.put(key, value);
        return filterWrap(rangeWrap(newParent(path, m)));
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
        List<String> boostedFields = esQuery.getBoostFields(null, null);

        if (boostedFields.isEmpty()) {
            if (ft.operator() == QueryTree.Operator.EQUALS) {
                return simpleQuery;
            }
            if (ft.operator() == QueryTree.Operator.NOT_EQUALS) {
                return mustNotWrap(simpleQuery);
            }
        }

        List<String> softFields = boostedFields.stream()
                .filter(f -> f.contains(JsonLd.getSEARCH_KEY()))
                .toList();
        List<String> exactFields = boostedFields.stream()
                .map(f -> f.replace(JsonLd.getSEARCH_KEY(), JsonLd.getSEARCH_KEY() + ".exact"))
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
        if (ft.operator() == QueryTree.Operator.EQUALS) {
            return shouldWrap(shouldClause);
        }
        if (ft.operator() == QueryTree.Operator.NOT_EQUALS) {
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

    private static String getQueryString(Map<String, String[]> queryParameters) {
        String[] q = queryParameters.get("_q");
        return q[0];
    }

    private static Map mustWrap(List o) {
        return boolWrap(newParent("must", o));
    }

    private static Map mustNotWrap(Object o) {
        return boolWrap(newParent("must_not", o));
    }

    private static Map shouldWrap(List l) {
        return boolWrap(newParent("should", l));
    }

    private static Map boolWrap(Map m) {
        return newParent("bool", m);
    }

    private static Map filterWrap(Map m) {
        return boolWrap(newParent("filter", m));
    }

    private static Map rangeWrap(Map m) {
        return newParent("range", m);
    }

    private static Map newParent(String key, Object o) {
        Map parent = new HashMap<>();
        parent.put(key, o);
        return parent;
    }

    private static String quoteIfPhrase(String s) {
        return s.matches(".*\\s.*") ? "\"" + s + "\"" : s;
    }
}

package whelk.search;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.util.Unicode;
import whelk.xlql.BadQueryException;
import whelk.xlql.QueryTreeBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class XLQLQuery {
    private Whelk whelk;
    private ESQuery esQuery;
    public QueryTreeBuilder queryTreeBuilder;

    public XLQLQuery(Whelk whelk) {
        this.whelk = whelk;
        this.esQuery = new ESQuery(whelk);
        this.queryTreeBuilder = new QueryTreeBuilder(whelk);
    }

    public Map doQuery(Map<String, String[]> queryParameters) {
        if (!queryParameters.containsKey("_q")) {
            throw new RuntimeException("Missing _q parameter");
        }
        Object queryTree = this.queryTreeBuilder.toQueryTree(getQueryString(queryParameters));
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
        if (queryTree instanceof QueryTreeBuilder.FreeText) {
            return esFreeText((QueryTreeBuilder.FreeText) queryTree);
        }
        if (queryTree instanceof QueryTreeBuilder.Field) {
            return esFilter((QueryTreeBuilder.Field) queryTree);
        }
        return buildEsQuery(queryTree, new HashMap<>());
    }

    public Map toMappings(Object sqt) {
        return buildMappings(sqt, sqt, new LinkedHashMap());
    }

    private Map buildMappings(Object sqt, Object sqtNode, Map mappingsNode) {
        if (sqtNode instanceof QueryTreeBuilder.FreeText) {
            mappingsNode = freeTextMapping((QueryTreeBuilder.FreeText) sqtNode);
        } else if (sqtNode instanceof QueryTreeBuilder.SimpleField) {
            mappingsNode = fieldMapping((QueryTreeBuilder.SimpleField) sqtNode);
        } else if (sqtNode instanceof QueryTreeBuilder.And) {
            List<Object> and = new ArrayList<>();
            for (Object c : ((QueryTreeBuilder.And) sqtNode).conjuncts()) {
                and.add(buildMappings(sqt, c, new LinkedHashMap()));
            }
            mappingsNode.put("and", and);
        } else if (sqtNode instanceof QueryTreeBuilder.Or) {
            List<Object> or = new ArrayList<>();
            for (Object c : ((QueryTreeBuilder.Or) sqtNode).disjuncts()) {
                or.add(buildMappings(sqt, c, new LinkedHashMap()));
            }
            mappingsNode.put("or", or);
        }

        Optional<Object> reducedTree = getReducedTree(sqt, sqtNode);
        // TODO: Empty tree --> ???
        String upUrl = reducedTree.isPresent() ? "/find?_q=" + treeToQueryString(reducedTree.get()) : "/find?q=*";
        mappingsNode.put("up", upUrl);

        return mappingsNode;
    }

    private Map mapping(String property, String value, QueryTreeBuilder.Operator operator) {
        Map m = new LinkedHashMap();
        m.put("variable", property);
        m.put("predicate", whelk.getJsonld().getVocabIndex().get(property));
        m.put("value", value);
        // TODO: use string instead of enum (Operator)
        m.put("operator", operator);
        return m;
    }

    private Map freeTextMapping(QueryTreeBuilder.FreeText ft) {
        return mapping("textQuery", ft.value(), ft.operator());
    }

    private Map fieldMapping(QueryTreeBuilder.SimpleField f) {
        return mapping(f.property(), f.value(), f.operator());
    }

    private Optional<Object> getReducedTree(Object qt, Object qtNode) {
        return Optional.ofNullable(excludeFromTree(qtNode, qt));
    }

    private Object excludeFromTree(Object nodeToExclude, Object tree) {
        if (nodeToExclude.equals(tree)) {
            return null;
        }
        if (tree instanceof QueryTreeBuilder.And) {
            List<Object> and = new ArrayList<>();
            for (Object c : ((QueryTreeBuilder.And) tree).conjuncts()) {
                Object elem = excludeFromTree(nodeToExclude, c);
                if (elem != null) {
                    and.add(elem);
                }
            }
            return and.size() > 1 ? new QueryTreeBuilder.And(and) : and.get(0);
        }
        if (tree instanceof QueryTreeBuilder.Or) {
            List<Object> or = new ArrayList<>();
            for (Object c : ((QueryTreeBuilder.Or) tree).disjuncts()) {
                Object elem = excludeFromTree(nodeToExclude, c);
                if (elem != null) {
                    or.add(elem);
                }
            }
            return or.size() > 1 ? new QueryTreeBuilder.Or(or) : or.get(0);
        }
        return tree;
    }

    private String treeToQueryString(Object sqt) {
        return buildQueryString(sqt, true);
    }

    private String buildQueryString(Object sqt, boolean topLevel) {
        if (sqt instanceof QueryTreeBuilder.And) {
            String andClause = ((QueryTreeBuilder.And) sqt).conjuncts()
                    .stream()
                    .map(this::buildQueryString)
                    .collect(Collectors.joining(" AND "));
            return topLevel ? andClause : "(" + andClause + ")";
        }
        if (sqt instanceof QueryTreeBuilder.Or) {
            String orClause = ((QueryTreeBuilder.Or) sqt).disjuncts()
                    .stream()
                    .map(this::buildQueryString)
                    .collect(Collectors.joining(" OR "));
            return topLevel ? orClause : "(" + orClause + ")";
        }
        if (sqt instanceof QueryTreeBuilder.FreeText) {
            return freeTextToString((QueryTreeBuilder.FreeText) sqt);
        }
        if (sqt instanceof QueryTreeBuilder.SimpleField) {
            return fieldToString((QueryTreeBuilder.SimpleField) sqt);
        }
        throw new RuntimeException("Unknown class of query tree node: " + sqt.getClass()); // Shouldn't be reachable
    }

    private String buildQueryString(Object qt) {
        return buildQueryString(qt, false);
    }

    private String freeTextToString(QueryTreeBuilder.FreeText ft) {
        return ft.operator() == QueryTreeBuilder.Operator.NOT_EQUALS ? "NOT " + ft.value() : ft.value();
    }

    private String fieldToString(QueryTreeBuilder.SimpleField f) {
        String sep = switch (f.operator()) {
            case EQUALS -> ": ";
            case NOT_EQUALS -> ": ";
            case GREATER_THAN_OR_EQUAL -> " >= ";
            case GREATER_THAN -> " > ";
            case LESS_THAN_OR_EQUAL -> " <= ";
            case LESS_THAN -> " < ";
        };
        String not = f.operator() == QueryTreeBuilder.Operator.NOT_EQUALS ? "NOT " : "";
        return not + f.property() + sep + f.value();
    }

    private Map buildEsQuery(Object qtNode, Map esQueryNode) {
        if (qtNode instanceof QueryTreeBuilder.And) {
            List<Object> must = new ArrayList<>();
            for (Object c : ((QueryTreeBuilder.And) qtNode).conjuncts()) {
                if (c instanceof QueryTreeBuilder.FreeText) {
                    must.add(esFreeText((QueryTreeBuilder.FreeText) c));
                } else if (c instanceof QueryTreeBuilder.Field) {
                    must.add(esFilter((QueryTreeBuilder.Field) c));
                } else {
                    must.add(buildEsQuery(c, new HashMap<>()));
                }
            }
            esQueryNode.putAll(mustWrap(must));
        } else if (qtNode instanceof QueryTreeBuilder.Or) {
            List should = new ArrayList<>();
            for (Object d : ((QueryTreeBuilder.Or) qtNode).disjuncts()) {
                if (d instanceof QueryTreeBuilder.FreeText) {
                    should.add(esFreeText((QueryTreeBuilder.FreeText) d));
                } else if (d instanceof QueryTreeBuilder.Field) {
                    should.add(esFilter((QueryTreeBuilder.Field) d));
                } else {
                    should.add(buildEsQuery(d, new HashMap<>()));
                }
            }
            esQueryNode.putAll(shouldWrap(should));
        }
        return esQueryNode;
    }

    private Map esFilter(QueryTreeBuilder.Field f) {
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

    private Map esFreeText(QueryTreeBuilder.FreeText ft) {
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
            if (ft.operator() == QueryTreeBuilder.Operator.EQUALS) {
                return simpleQuery;
            }
            if (ft.operator() == QueryTreeBuilder.Operator.NOT_EQUALS) {
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
        if (ft.operator() == QueryTreeBuilder.Operator.EQUALS) {
            return shouldWrap(shouldClause);
        }
        if (ft.operator() == QueryTreeBuilder.Operator.NOT_EQUALS) {
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

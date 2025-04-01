package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.Operator;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.search2.Operator.EQUALS;

public record FreeText(TextQuery textQuery, Operator operator, String value, Collection<String> boostFields) implements Node {
    public FreeText(Operator operator, String value, JsonLd jsonLd) {
        this(new TextQuery(jsonLd), operator, value);
    }
    public FreeText(TextQuery textQuery, Operator operator, String value) { this(textQuery, operator, value, List.of()); }

    @Override
    // TODO: Review/refine this. So far it's basically just copy-pasted from old search code (EsQuery)
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath) {
        String s = value;
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

        if (boostFields.isEmpty()) {
            if (operator() == Operator.EQUALS) {
                return simpleQuery;
            }
            if (operator() == Operator.NOT_EQUALS) {
                return mustNotWrap(simpleQuery);
            }
        }

        List<String> softFields = boostFields.stream()
                .filter(f -> f.contains(JsonLd.SEARCH_KEY))
                .toList();
        List<String> exactFields = boostFields.stream()
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

        if (operator() == Operator.EQUALS) {
            return shouldWrap(shouldClause);
        }
        if (operator() == Operator.NOT_EQUALS) {
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

    @Override
    public Map<String, Object> toEs(Function<String, Optional<String>> getNestedPath, Collection<String> boostFields) {
        String s = value;
        s = Unicode.normalizeForSearch(s);
        boolean isSimple = ESQuery.isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = ESQuery.escapeNonSimpleQueryString(s);
        }
        String queryString = s;

        if (boostFields.isEmpty()) {
            return wrap(buildSimpleQuery(queryMode, queryString));
        }

        List<String> basicBoostFields = boostFields.stream().filter(f -> Pattern.matches(".+\\^(0\\.)?[0-9]+", f)).toList();
        List<String> functionBoostFields = boostFields.stream().filter(f -> Pattern.matches(".+\\^(0\\.)?[0-9]+\\(.+\\)", f)).toList();

        List<Map<String, Object>> queries = new ArrayList<>();

        queries.add(buildSimpleQuery(queryMode, queryString, basicBoostFields));

        Map<String, List<String>> fieldsGroupedByFunction = new HashMap<>();
        for (String bf : functionBoostFields) {
            String[] split = bf.split("\\(");
            String field = split[0];
            String multiplier = split[1].replace(")", "");
            String function = "_score * " + multiplier;
            fieldsGroupedByFunction.computeIfAbsent(function, k -> new ArrayList<>()).add(field);
        }

        fieldsGroupedByFunction.forEach((function, fields) -> {
            Map<String, Object> scriptScoreQuery = Map.of(
                    "script_score", Map.of(
                            "query", buildSimpleQuery(queryMode, queryString, fields),
                            "script", Map.of("source", function)));
            queries.add(scriptScoreQuery);
        });

        return wrap(queries.size() == 1 ? queries.getFirst() : shouldWrap(queries));
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return new FreeText(textQuery, operator, value, getBoostFields.apply(rulingTypes));
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return this;
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", textQuery.definition());
        m.put(operator.termKey, value);
        m.put("up", qt.makeUpLink(this, nonQueryParams));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return operator == Operator.NOT_EQUALS
                ? "NOT " + value :
                value;
    }

    public boolean isWild() {
        return operator == EQUALS && Operator.WILDCARD.equals(value);
    }

    public static class TextQuery extends Property {
        TextQuery(JsonLd jsonLd) {
            super("textQuery", jsonLd);
        }

        // For test only
        TextQuery(Map<String, Object> definition) {
            super("textQuery", definition, null);
        }
    }

    private Map<String, Object> wrap(Map<String, Object> query) {
        if (operator == Operator.EQUALS) {
            return query;
        }
        if (operator == Operator.NOT_EQUALS) {
            return mustNotWrap(query);
        }
        throw new RuntimeException("Invalid operator"); // Not reachable
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString) {
        return buildSimpleQuery(queryMode, queryString, List.of());
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, Collection<String> fields) {
        var query = new HashMap<>();
        query.put("query", queryString);
        query.put("analyze_wildcard", true);
        query.put("default_operator", "AND");
        if (!fields.isEmpty()) {
            query.put("fields", fields);
        }
        return Map.of(queryMode, query);
    }
}

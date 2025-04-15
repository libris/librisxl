package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.search2.QueryParams;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.search2.Operator.EQUALS;

public record FreeText(Property.TextQuery textQuery, Operator operator, String value) implements Node {
    public FreeText(String value) {
        this(null, EQUALS, value);
    }

    @Override
    public Map<String, Object> toEs(EsMappings esMappings, Collection<String> boostFields) {
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

        Map<String, Float> basicBoostFields = new LinkedHashMap<>();
        Map<String, String> functionBoostFields = new LinkedHashMap<>();

        for (String bf : boostFields) {
            try {
                String field = bf.substring(0, bf.indexOf('^'));
                String boost = bf.substring(bf.indexOf('^') + 1);
                if (boost.contains("(")) {
                    Float basicBoost = Float.parseFloat(boost.substring(0, boost.indexOf('(')));
                    String function = boost.substring(boost.indexOf('(') + 1, boost.lastIndexOf(')'));
                    basicBoostFields.put(field, basicBoost);
                    functionBoostFields.put(field, function);
                } else {
                    basicBoostFields.put(field, Float.parseFloat(boost));
                }
            } catch (Exception ignored) {
            }
        }

        List<Map<String, Object>> queries = new ArrayList<>();

        if (functionBoostFields.size() != basicBoostFields.size()) {
            queries.add(buildBasicBoostQuery(queryMode, queryString, basicBoostFields, functionBoostFields));
        }
        queries.addAll(buildFunctionBoostQueries(queryMode, queryString, basicBoostFields, functionBoostFields));

        return wrap(queries.size() == 1 ? queries.getFirst() : shouldWrap(queries));
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return this;
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", textQuery.definition());
        m.put(operator.termKey, value);
        m.put("up", makeUpLink(qt, this, queryParams));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        return operator == Operator.NOT_EQUALS
                ? "NOT " + value :
                value;
    }

    @Override
    public Node getInverse() {
        return new FreeText(textQuery, operator.getInverse(), value);
    }

    @Override
    public boolean isFreeTextNode() {
        return operator.equals(EQUALS);
    }

    @Override
    public String toString() {
        return operator == Operator.NOT_EQUALS
                ? "NOT " + value :
                value;
    }

    public FreeText replace(String replacement) {
        return new FreeText(textQuery, operator, replacement);
    }

    public boolean isWild() {
        return operator == EQUALS && Operator.WILDCARD.equals(value);
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

    private Map<String, Object> buildBasicBoostQuery(String queryMode, String queryString, Map<String, Float> basicBoostFields, Map<String, String> functionBoostFields) {
        Map<String, Float> boostFields = new LinkedHashMap<>();
        basicBoostFields.forEach((field, boost) -> boostFields.put(field, functionBoostFields.containsKey(field) ? 0 : boost));
        return buildSimpleQuery(queryMode, queryString, boostFields);
    }

    private List<Map<String, Object>> buildFunctionBoostQueries(String queryMode, String queryString, Map<String, Float> basicBoostFields, Map<String, String> functionBoostFields) {
        List<Map<String, Object>> queries = new ArrayList<>();

        Map<String, List<String>> fieldsGroupedByFunction = new LinkedHashMap<>();
        functionBoostFields.forEach((field, function) -> fieldsGroupedByFunction.computeIfAbsent(function, v -> new ArrayList<>()).add(field));

        fieldsGroupedByFunction.forEach((function, fields) -> {
            Map<String, Float> boostFields = new LinkedHashMap<>();
            basicBoostFields.forEach((f, boost) -> boostFields.put(f, fields.contains(f) ? boost : 0));

            Map<String, Object> scriptScoreQuery = Map.of(
                    "script_score", Map.of(
                            "query", buildSimpleQuery(queryMode, queryString, boostFields),
                            "script", Map.of("source", "_score * " + function)));

            queries.add(scriptScoreQuery);
        });

        return queries;
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

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, Map<String, Float> fields) {
        var fieldsStrings = fields.entrySet().stream().map(e -> e.getKey() + "^" + e.getValue()).toList();
        return buildSimpleQuery(queryMode, queryString, fieldsStrings);
    }
}

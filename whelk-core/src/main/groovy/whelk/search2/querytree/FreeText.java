package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search.ESQuery;
import whelk.search2.EsBoost;
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

import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.isQuoted;
import static whelk.search2.QueryUtil.isSimple;
import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.quote;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.search2.Operator.EQUALS;

public record FreeText(Property.TextQuery textQuery, Operator operator, String value) implements Node {
    public FreeText(String value) {
        this(null, EQUALS, value);
    }

    @Override
    public Map<String, Object> toEs(EsMappings esMappings, EsBoost.Config boostConfig) {
        if (boostConfig.suggest()) {
            var shouldClauses = List.of(
                    // Make a prefix query (e.g. add a trailing * to query string) to get suggestions
                    replace(value + Operator.WILDCARD)._toEs(boostConfig),
                    // Also make a non-prefix query to get higher relevancy score for exact matches
                    _toEs(boostConfig)
            );
            return shouldWrap(shouldClauses);
        }
        return _toEs(boostConfig);
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
    public boolean shouldContributeToEsScore() {
        return operator == EQUALS && !Operator.WILDCARD.equals(value);
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

    private Map<String, Object> _toEs(EsBoost.Config boostConfig) {
        String s = value;
        s = Unicode.normalizeForSearch(s);
        boolean isSimple = isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = ESQuery.escapeNonSimpleQueryString(s);
        }
        String queryString = s;

        List<String> boostFields = boostConfig.boostFields();

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

        var queries = buildQueries(queryMode, queryString, basicBoostFields, functionBoostFields);

        if (!isQuoted(queryString) && isMultiWord(queryString)) {
            List<String> simplePhrases = getSimplePhrases(queryString);
            if (!simplePhrases.isEmpty()) {
                Integer phraseBoostDivisor = boostConfig.phraseBoostDivisor();
                if (phraseBoostDivisor != null) {
                    basicBoostFields = basicBoostFields.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> e.getValue() / phraseBoostDivisor));
                }
                for (String phrase : simplePhrases) {
                    queries.addAll(buildQueries("query_string", phrase, basicBoostFields, functionBoostFields));
                }
            }
        }

        return wrap(queries.size() == 1 ? queries.getFirst() : shouldWrap(queries));
    }

    private boolean isMultiWord(String s) {
        return s.matches(".*\\S\\s+\\S.*");
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

    private List<Map<String, Object>> buildQueries(String queryMode, String queryString, Map<String, Float> basicBoostFields, Map<String, String> functionBoostFields) {
        List<Map<String, Object>> queries = new ArrayList<>();

        if (functionBoostFields.size() != basicBoostFields.size()) {
            queries.add(buildBasicBoostQuery(queryMode, queryString, basicBoostFields, functionBoostFields));
        }
        queries.addAll(buildFunctionBoostQueries(queryMode, queryString, basicBoostFields, functionBoostFields));

        return queries;
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

        String lengthNormMultiplier = isQuoted(queryString) ? queryString.split("\\s+").length + " * " : "";

        fieldsGroupedByFunction.forEach((function, fields) -> {
            Map<String, Float> boostFields = new LinkedHashMap<>();
            basicBoostFields.forEach((f, boost) -> boostFields.put(f, fields.contains(f) ? boost : 0));

            Map<String, Object> scriptScoreQuery = Map.of(
                    "script_score", Map.of(
                            "query", buildSimpleQuery(queryMode, queryString, boostFields),
                            "script", Map.of("source", lengthNormMultiplier + function)));

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
        if (queryMode.equals("query_string")) {
            query.put("type", "most_fields");
        }
        return Map.of(queryMode, query);
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, Map<String, Float> fields) {
        var fieldsStrings = fields.entrySet().stream().map(e -> e.getKey() + "^" + e.getValue()).toList();
        return buildSimpleQuery(queryMode, queryString, fieldsStrings);
    }

    private List<String> getSimplePhrases(String queryString) {
        String[] tokens = queryString.split("\\s+");

        List<String> simplePhrases = new ArrayList<>();
        List<String> currentSimpleSequence = new ArrayList<>();

        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (isSimple(token) && !token.endsWith(Operator.WILDCARD)) {
                currentSimpleSequence.add(token);
            } else {
                if (currentSimpleSequence.size() > 1) {
                    simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
                }
                currentSimpleSequence.clear();
            }
            if (i == tokens.length - 1 && currentSimpleSequence.size() > 1)  {
                simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
            }
        }

        return simplePhrases;
    }
}

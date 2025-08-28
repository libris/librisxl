package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.Operator;
import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static whelk.search2.Operator.NOT_EQUALS;
import static whelk.search2.Query.Connective.AND;
import static whelk.search2.Query.Connective.OR;
import static whelk.search2.QueryUtil.isQuoted;
import static whelk.search2.QueryUtil.isSimple;
import static whelk.search2.QueryUtil.makeUpLink;
import static whelk.search2.QueryUtil.mustNotWrap;
import static whelk.search2.QueryUtil.parenthesize;
import static whelk.search2.QueryUtil.quote;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.search2.Operator.EQUALS;

public record FreeText(Property.TextQuery textQuery, boolean negate, List<Token> tokens,
                       Query.Connective connective) implements Node, Value {
    public FreeText(Property.TextQuery textQuery, boolean negate, Token token) {
        this(textQuery, negate, List.of(token), AND);
    }

    public FreeText(Token token) {
        this(null, false, token);
    }

    public FreeText(String s) {
        this(new Token.Raw(s));
    }

    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return _toEs(tokens, esSettings.boost().fieldBoost());
    }

    public Map<String, Object> toEs(ESSettings.Boost.FieldBoost boostSettings) {
        return _toEs(tokens, boostSettings);
    }

    @Override
    public Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return this;
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, QueryParams queryParams) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("property", textQuery != null ? textQuery.definition() : Map.of());
        m.put(negate ? NOT_EQUALS.termKey : EQUALS.termKey, queryForm());
        m.put("up", makeUpLink(qt, this, queryParams));
        return m;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        String s = isMultiToken() && (negate || (!topLevel && connective.equals(OR)))
                ? parenthesize(joinTokens())
                : joinTokens();
        return negate ? "NOT " + s : s;
    }

    @Override
    public String queryForm() {
        return joinTokens();
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public Node getInverse() {
        return new FreeText(textQuery, !negate, tokens, connective);
    }

    @Override
    public boolean isMultiToken() {
        return tokens.size() > 1;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FreeText ft && ft.toString().equals(toString());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toString());
    }

    public boolean isWild() {
        return Operator.WILDCARD.equals(toString());
    }

    public Optional<Token> getCurrentlyEditedToken(int cursorPos) {
        return tokens.stream()
                .filter(t -> cursorPos > t.offset() && cursorPos <= t.offset() + t.value().length())
                .findFirst();
    }

    public FreeText withTokens(List<Token> tokens) {
        return new FreeText(textQuery, negate, tokens, connective);
    }

    private String joinTokens() {
        return switch (connective) {
            case AND -> joinTokens(tokens, " ");
            case OR -> joinTokens(tokens, " OR ");
        };
    }

    private static String joinTokens(List<Token> tokens, String delimiter) {
        return tokens.stream().map(Token::formatted).collect(Collectors.joining(delimiter));
    }

    private Map<String, Object> _toEs(List<Token> tokens, ESSettings.Boost.FieldBoost boostSettings) {
        String s = joinTokens(tokens, " ");
        s = Unicode.normalizeForSearch(s);

        // TODO search for original string OR stripped string?
        if (Unicode.looksLikeIsbn(s) && s.contains("-")) {
            s = s.replace("-", "");
        }

        boolean isSimple = isSimple(s);
        String queryMode = isSimple ? "simple_query_string" : "query_string";
        if (!isSimple) {
            s = QueryUtil.escapeNonSimpleQueryString(s);
        }
        String queryString = s;

        if (boostSettings.fields().isEmpty()) {
            return wrap(buildSimpleQuery(queryMode, queryString, boostSettings));
        }

        var queries = buildQueries(queryMode, queryString, boostSettings);

        List<String> simplePhrases = getSimplePhrases(tokens);
        if (!simplePhrases.isEmpty()) {
            for (String phrase : simplePhrases) {
                queries.addAll(buildQueries("query_string", phrase, boostSettings));
            }
        }

        return wrap(queries.size() == 1 ? queries.getFirst() : shouldWrap(queries));
    }

    private Map<String, Object> wrap(Map<String, Object> query) {
        return negate ? mustNotWrap(query) : query;
    }

    private List<Map<String, Object>> buildQueries(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings) {
        List<Map<String, Object>> queries = new ArrayList<>();

        if (boostSettings.fields().stream().anyMatch(f -> f.scriptScore().isEmpty())) {
            queries.add(buildBasicBoostQuery(queryMode, queryString, boostSettings));
        }
        queries.addAll(buildScriptScoreQueries(queryMode, queryString, boostSettings));

        return queries;
    }

    private Map<String, Object> buildBasicBoostQuery(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings) {
        Map<String, Float> boostFields = new LinkedHashMap<>();
        boostSettings.fields().forEach(f -> {
            float boost = f.scriptScore().isEmpty() ? f.boost() : 0;
            if (isPhrase(queryString)) {
                boost = boost / boostSettings.phraseBoostDivisor();
            }
            boostFields.put(f.name(), boost);
            if (boostSettings.includeExactFields()) {
                boostFields.put(f.name() + ESSettings.Boost.EXACT_SUFFIX, boost);
            }
        });
        return buildSimpleQuery(queryMode, queryString, boostSettings, boostFields);
    }

    private List<Map<String, Object>> buildScriptScoreQueries(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings) {
        List<Map<String, Object>> queries = new ArrayList<>();

        var scriptScores = boostSettings.fields().stream()
                .map(f -> f.scriptScore())
                .distinct()
                .filter(f -> !f.isEmpty())
                .toList();

        scriptScores.forEach(scriptScore -> {
            Map<String, Float> boostFields = new LinkedHashMap<>();

            boostSettings.fields().forEach(f -> {
                float boost = f.scriptScore().equals(scriptScore) ? f.boost() : 0;
                if (isPhrase(queryString)) {
                    boost = boost / boostSettings.phraseBoostDivisor();
                }
                boostFields.put(f.name(), boost);
                if (boostSettings.includeExactFields()) {
                    boostFields.put(f.name() + ESSettings.Boost.EXACT_SUFFIX, boost);
                }
            });

            String lengthNormMultiplier = "length normalizer".equals(scriptScore.name()) && isQuoted(queryString)
                    ? queryString.split("\\s+").length + " * "
                    : "";
            String function = lengthNormMultiplier + scriptScore.function();
            String source = scriptScore.applyIf() == null
                    ? function
                    : scriptScore.applyIf() + " ? " + function + " : _score";

            Map<String, Object> scriptScoreQuery = Map.of(
                    "script_score", Map.of(
                            "query", buildSimpleQuery(queryMode, queryString, boostSettings, boostFields),
                            "script", Map.of("source", source)));

            queries.add(scriptScoreQuery);
        });

        return queries;
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings) {
        return buildSimpleQuery(queryMode, queryString, boostSettings, List.of());
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings, Collection<String> fields) {
        var query = new HashMap<>();
        query.put("query", queryString);
        query.put("analyze_wildcard", boostSettings.analyzeWildcard());
        query.put("default_operator", connective.name());
        if (!fields.isEmpty()) {
            query.put("fields", fields);
            if (queryMode.equals("query_string") && fields.size() > 1 && boostSettings.multiMatchType() != null) {
                query.put("type", boostSettings.multiMatchType());
            }
        }
        return Map.of(queryMode, query);
    }

    private Map<String, Object> buildSimpleQuery(String queryMode, String queryString, ESSettings.Boost.FieldBoost boostSettings, Map<String, Float> fields) {
        var fieldsStrings = fields.entrySet().stream().map(e -> e.getKey() + "^" + e.getValue()).toList();
        return buildSimpleQuery(queryMode, queryString, boostSettings, fieldsStrings);
    }

    private static List<String> getSimplePhrases(List<Token> tokens) {
        List<String> simplePhrases = new ArrayList<>();
        List<String> currentSimpleSequence = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (!token.isQuoted() && isSimple(token.value()) && !token.value().endsWith(Operator.WILDCARD)) {
                currentSimpleSequence.add(token.value());
            } else {
                if (currentSimpleSequence.size() > 1) {
                    simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
                }
                currentSimpleSequence.clear();
            }
            if (i == tokens.size() - 1 && currentSimpleSequence.size() > 1) {
                simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
            }
        }

        return simplePhrases;
    }

    private static boolean isPhrase(String s) {
        return s.contains(" ");
    }
}

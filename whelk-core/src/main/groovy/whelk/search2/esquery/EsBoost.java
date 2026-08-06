package whelk.search2.esquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class EsBoost {
    private final List<Field> freeTextFields;
    private final FreeTextQuerySettings freeTextQuerySettings;
    private final FieldedQuerySettings fieldedQuerySettings; // TODO: Improve relevancy for targeted queries
    private final FunctionScore functionScore;
    private final List<EsQuery.ConstantScore> constantScores;

    public EsBoost(Map<?, ?> settings) {
        this.freeTextFields = loadFreeTextFields(settings);
        this.freeTextQuerySettings = loadFreeTextQuerySettings(settings);
        this.fieldedQuerySettings = loadFieldedQuerySettings(settings); // TODO: Separate freetext vs fielded query settings in config file
        this.functionScore = loadFunctionScore(settings);
        this.constantScores = loadConstantsScores(settings);
    }

    public Map<String, Object> functionScore() {
        return functionScore.dsl();
    }

    public Map<String, Object> constantScore() {
        List<EsQuery> subQueries = new ArrayList<>(constantScores);
        // Since the constant clauses are only for scoring, and we don't actually require any of the filters to match,
        // include a match_all clause to make sure that the overall query never fails due to all constant queries failing.
        subQueries.add(new EsQuery.MatchAll());
        return new EsQuery.Should(subQueries).dsl();
    }

    public List<Field> freeTextFields() {
        return freeTextFields;
    }

    public FreeTextQuerySettings freeTextQuerySettings() {
        return freeTextQuerySettings;
    }

    public FieldedQuerySettings fieldedQuerySettings() {
        return fieldedQuerySettings;
    }

    public sealed interface TextQuerySettings permits FreeTextQuerySettings, FieldedQuerySettings {
        boolean analyzeWildcard();
        String quoteFieldSuffix();
        int phraseBoostDivisor();

        default boolean boostPhrase() {
            return phraseBoostDivisor() != 0;
        }
        default boolean includeExactFields() {
            return !quoteFieldSuffix().isEmpty();
        }
    }

    public record FreeTextQuerySettings(boolean analyzeWildcard,
                                        String quoteFieldSuffix,
                                        int phraseBoostDivisor) implements TextQuerySettings {
    }

    public record FieldedQuerySettings(boolean analyzeWildcard,
                                       String quoteFieldSuffix,
                                       int phraseBoostDivisor,
                                       float defaultBoostFactor) implements TextQuerySettings {
        public static FieldedQuerySettings defaultSettings() {
            return new FieldedQuerySettings(false, "", 0, 1);
        }
    }

    public record Field(String name, float boost, ScriptScoreNormalizer normalizer) {
        Field(Map<?, ?> settings) {
            this((String) settings.get("name"),
                    getAsFloat(settings, "boost"),
                    settings.containsKey("script_score")
                            ? new ScriptScoreNormalizer(getAsMap(settings, "script_score"))
                            : null);
        }

        public Field(String name, float boost) {
            this(name, boost, null);
        }

        public Field withBoost(float boost) {
            return new Field(name, boost, normalizer);
        }

        public static Field unboosted(String name) {
            return new Field(name, 1);
        }

        public static Field nonScoring(String name) {
            return new Field(name, 0);
        }
    }

    public record ScriptScoreNormalizer(String name, String function, String applyIf) {
        ScriptScoreNormalizer(Map<?, ?> settings) {
            this((String) settings.get("name"),
                    (String) settings.get("function"),
                    (String) settings.get("apply_if"));
        }

        public String source() {
            return applyIf == null ? function : applyIf + " ? " + function + " : _score";
        }
    }

    private record FunctionScore(List<EsQuery.ScoreFunction> functions, String scoreMode, String boostMode) {
        Map<String, Object> dsl() {
            if (functions.isEmpty()) {
                return Map.of();
            }
            return Map.of("function_score",
                    Map.of("query", new EsQuery.MatchAll().dsl(),
                            "functions", functions.stream().map(EsQuery.ScoreFunction::dsl).toList(),
                            "score_mode", "sum",
                            "boost_mode", "sum"));
        }
    }

    private static List<Field> loadFreeTextFields(Map<?, ?> settings) {
        Map<?, ?> boostSettings = getAsMap(settings, "field_boost");
        return getAsStream(boostSettings, "fields")
                .map(Map.class::cast)
                .map(Field::new)
                .toList();
    }

    private static FreeTextQuerySettings loadFreeTextQuerySettings(Map<?, ?> settings) {
        Map<?, ?> boostSettings = getAsMap(settings, "field_boost");
        return new FreeTextQuerySettings(
                getOrDefault(boostSettings, "analyze_wildcard", false),
                getOrDefault(boostSettings, "quote_field_suffix", ""),
                getOrDefault(boostSettings, "phrase_boost_divisor", 0)
        );
    }

    private static FieldedQuerySettings loadFieldedQuerySettings(Map<?, ?> settings) {
        Map<?, ?> boostSettings = getAsMap(settings, "field_boost");
        return new FieldedQuerySettings(
                getOrDefault(boostSettings, "analyze_wildcard", false),
                getOrDefault(boostSettings, "quote_field_suffix", ""),
                getOrDefault(boostSettings, "phrase_boost_divisor", 0),
                getOrDefault(boostSettings, "default_boost_factor", 1)
        );
    }

    private static FunctionScore loadFunctionScore(Map<?, ?> settings) {
        Map<?, ?> functionScoreSettings = getAsMap(settings, "function_score");
        List<EsQuery.ScoreFunction> scoreFunctions = getAsStream(functionScoreSettings, "functions")
                .map(Map.class::cast)
                .map(EsBoost::loadScoreFunction)
                .filter(Objects::nonNull)
                .toList();
        return new FunctionScore(scoreFunctions,
                (String) functionScoreSettings.get("score_mode"),
                (String) functionScoreSettings.get("boost_mode"));
    }

    private static EsQuery.ScoreFunction loadScoreFunction(Map<?, ?> settings) {
        return EsQuery.FieldValueFactor.key().equals(settings.get("type"))
                ? new EsQuery.FieldValueFactor(getAsMap(settings, "params"), getAsFloat(settings, "weight"))
                : null;
    }

    private static List<EsQuery.ConstantScore> loadConstantsScores(Map<?, ?> settings) {
        return getAsStream(settings, "constant_score")
                .map(Map.class::cast)
                .map(m -> {
                    String f = (String) m.get("field");
                    String v = (String) m.get("value");
                    float s = getAsFloat(m, "score");
                    return new EsQuery.ConstantScore(new EsQuery.TermQuery(f, v), s);
                })
                .toList();
    }

    private static Map<String, Object> getAsMap(Map<?, ?> m, String k) {
        return getOrDefault(m, k, Map.of());
    }

    private static float getAsFloat(Map<?, ?> m, String k) {
        return ((Number) m.get(k)).floatValue();
    }

    private static Stream<?> getAsStream(Map<?, ?> m, String k) {
        return getOrDefault(m, k, List.of()).stream();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getOrDefault(Map<?, ?> m, String k, T defaultTo) {
        return m.containsKey(k) ? (T) m.get(k) : defaultTo;
    }
}

package whelk.search2;

import whelk.Whelk;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.util.Jackson.mapper;

public class ESSettings {
    private static final String BOOST_SETTINGS_FILE = "libris_search_boost.json";

    private EsMappings mappings;
    private final Boost boost;

    private int maxItems;

    public ESSettings(Whelk whelk) {
        if (whelk.elastic != null) {
            this.mappings = new EsMappings(whelk.elastic.getMappings());
            this.maxItems = whelk.elastic.maxResultWindow;
        }
        this.boost = loadBoostSettings();
    }

    // For test only
    public ESSettings(EsMappings mappings, Boost boost) {
       this(mappings, boost, 1);
    }

    public ESSettings withBoostSettings(Boost boost) {
        return new ESSettings(mappings, boost, maxItems);
    }

    private ESSettings(EsMappings mappings, Boost boost, int maxItems) {
        this.mappings = mappings;
        this.boost = boost;
        this.maxItems = maxItems;
    }

    public boolean isConfigured() {
        return mappings != null;
    }

    public EsMappings mappings() {
        return mappings;
    }

    public Boost boost() {
        return boost;
    }

    public int maxItems() {
        return maxItems;
    }

    public Boost loadBoostSettings() {
        Map<?, ?> settings = toMap(Boost.class.getClassLoader().getResourceAsStream(BOOST_SETTINGS_FILE));
        return new Boost(settings);
    }

    public static Boost loadBoostSettings(String json) {
        return new Boost(toMap(json));
    }

    public record Boost(FieldBoost fieldBoost, FunctionScore functionScore, ConstantScore constantScore) {
        public static final String EXACT_SUFFIX = ".exact";

        public Boost(Map<?, ?> settings) {
            this(FieldBoost.load(settings), FunctionScore.load(settings), ConstantScore.load(settings));
        }

        public record FieldBoost(List<Field> fields,
                          float defaultBoostFactor,
                          int phraseBoostDivisor,
                          String multiMatchType,
                          boolean analyzeWildcard,
                          boolean includeExactFields) {
            public record Field(String name, float boost, ScriptScore scriptScore) {
                Field(Map<?, ?> settings) {
                    this((String) settings.get("name"), getAsFloat(settings, "boost"), new ScriptScore(getAsMap(settings, "script_score")));
                }

                public record ScriptScore(String name, String function, String applyIf) {
                    ScriptScore(Map<?, ?> settings) {
                        this((String) settings.get("name"), (String) settings.get("function"), (String) settings.get("apply_if"));
                    }

                    public boolean isEmpty() {
                        return function == null;
                    }
                }
            }

            public FieldBoost withField(String name) {
                return withField(name, defaultBoostFactor);
            }

            public FieldBoost withField(String name, float boost) {
                var fieldSettings = new HashMap<>() {{
                    put("name", name);
                    put("boost", boost);
                }};
                return withFields(List.of(fieldSettings));
            }

            public FieldBoost withFields(List<Map<?, ?>> fieldSettings) {
                List<Field> fields = fieldSettings.stream().map(Field::new).toList();
                return new FieldBoost(fields, defaultBoostFactor, phraseBoostDivisor, multiMatchType, analyzeWildcard, includeExactFields);
            }

            private static FieldBoost load(Map<?, ?> settings) {
                Map<?, ?> fieldBoostSettings = getAsMap(settings, "field_boost");
                List<Field> fields = getAsStream(fieldBoostSettings, "fields")
                        .map(Map.class::cast)
                        .map(Field::new)
                        .toList();
                return new FieldBoost(fields,
                        fieldBoostSettings.containsKey("default_boost_factor") ? getAsFloat(fieldBoostSettings, "default_boost_factor") : 1,
                        getOrDefault(fieldBoostSettings, "phrase_boost_divisor", 1),
                        (String) fieldBoostSettings.get("multi_match_type"),
                        getOrDefault(fieldBoostSettings, "analyze_wildcard", false),
                        getOrDefault(fieldBoostSettings, "include_exact_fields", false)
                );
            }
        }

        record FunctionScore(List<ScoreFunction> functions, String scoreMode, String boostMode) {
            sealed interface ScoreFunction permits ScoreFunction.FieldValueFactor {
                record FieldValueFactor(Map<?, ?> params, float weight) implements ScoreFunction {
                    @Override
                    public Map<String, Object> toEs() {
                        return Map.of(key(), params,
                                "weight", weight);
                    }

                    public static String key() {
                        return "field_value_factor";
                    }
                }

                Map<String, Object> toEs();

                static ScoreFunction load(Map<?, ?> settings) {
                    return FieldValueFactor.key().equals(settings.get("type"))
                            ? new FieldValueFactor(getAsMap(settings, "params"), getAsFloat(settings, "weight"))
                            : null;
                }
            }

            public Map<String, Object> toEs() {
                if (functions.isEmpty()) {
                    return Map.of();
                }
                return Map.of("function_score",
                        Map.of("query", matchAllClause(),
                                "functions", functions.stream().map(ScoreFunction::toEs).toList(),
                                "score_mode", "sum",
                                "boost_mode", "sum"));
            }

            private static FunctionScore load(Map<?, ?> settings) {
                Map<?, ?> functionScoreSettings = getAsMap(settings, "function_score");
                List<FunctionScore.ScoreFunction> scoreFunctions = getAsStream(functionScoreSettings, "functions")
                        .map(Map.class::cast)
                        .map(FunctionScore.ScoreFunction::load)
                        .filter(Objects::nonNull)
                        .toList();
                return new FunctionScore(scoreFunctions,
                        (String) functionScoreSettings.get("score_mode"),
                        (String) functionScoreSettings.get("boost_mode"));
            }
        }

        record ConstantScore(List<Constant> constants) {
            record Constant(String field, String value, float score) {
                Constant(Map<?, ?> settings) {
                    this((String) settings.get("field"), (String) settings.get("value"), getAsFloat(settings, "score"));
                }

                Map<String, Object> toEs() {
                    return Map.of(
                            "constant_score", Map.of(
                                    "filter", Map.of("term", Map.of(field, value)),
                                    "boost", score)
                    );
                }
            }

            public Map<String, Object> toEs() {
                var queries = constants.stream().map(Constant::toEs).collect(Collectors.toList());
                // Since the constant clauses are only for scoring, and we don't actually require any of the filters to match,
                // include a match_all clause to make sure that the overall query never fails due to all constant queries failing.
                queries.add(matchAllClause());
                return QueryUtil.shouldWrap(queries);
            }

            private static ConstantScore load(Map<?, ?> settings) {
                List<ConstantScore.Constant> constants = getAsStream(settings, "constant_score")
                        .map(Map.class::cast)
                        .map(ConstantScore.Constant::new)
                        .toList();
                return new ConstantScore(constants);
            }
        }

        private static Map<String, Object> matchAllClause() {
            return Map.of("match_all", Map.of());
        }

        private static Stream<?> getAsStream(Map<?, ?> m, String k) {
            return getOrDefault(m, k, List.of()).stream();
        }

        private static Map<String, Object> getAsMap(Map<?, ?> m, String k) {
            return getOrDefault(m, k, Map.of());
        }

        private static float getAsFloat(Map<?, ?> m, String k) {
            return ((Number) m.get(k)).floatValue();
        }

        @SuppressWarnings("unchecked")
        private static <T> T getOrDefault(Map<?, ?> m, String k, T defaultTo) {
            return m.containsKey(k) ? (T) m.get(k) : defaultTo;
        }
    }

    private static Map<?, ?> toMap(Object json) {
        try {
            if (json instanceof String) {
                return mapper.readValue((String) json, Map.class);
            } else if (json instanceof InputStream) {
                return mapper.readValue((InputStream) json, Map.class);
            }
        } catch (IOException ignored) {
            return Map.of();
        }
        return Map.of();
    }
}

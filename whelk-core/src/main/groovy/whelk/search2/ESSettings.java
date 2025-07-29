package whelk.search2;

import whelk.Whelk;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static whelk.util.Jackson.mapper;

public class ESSettings {
    private static final String BOOST_SETTINGS_FILE = "boost_settings.json";

    EsMappings mappings;
    Boost boost;

    private int maxItems;

    public ESSettings(Whelk whelk) {
        if (whelk.elastic != null) {
            this.mappings = new EsMappings(whelk.elastic.getMappings());
            this.maxItems = whelk.elastic.maxResultWindow;
        }
        this.boost = loadBoostSettings();
    }

    public boolean isConfigured() {
        return mappings != null;
    }

    public int maxItems() {
        return maxItems;
    }

    public Boost loadBoostSettings() {
        Map<?, ?> settings = toMap(Boost.class.getClassLoader().getResourceAsStream(BOOST_SETTINGS_FILE));
        return new Boost(settings);
    }

    public record Boost(FieldBoost fieldBoost, FunctionScore functionScore, ConstantScore constantScore) {
        Boost(Map<?, ?> settings) {
            this(FieldBoost.load(settings), FunctionScore.load(settings), ConstantScore.load(settings));
        }

        record FieldBoost(List<Field> fields,
                          int defaultBoostFactor,
                          int phraseBoostDivisor,
                          String multiMatchType,
                          boolean analyzeWildcard,
                          boolean includeExactFields) {
            record Field(String name, int boost, ScriptScore scriptScore) {
                Field(Map<?, ?> settings) {
                    this((String) settings.get("name"), (int) settings.get("boost"), new ScriptScore(getAsMap(settings, "script_score")));
                }

                record ScriptScore(String function, String applyIf) {
                    ScriptScore(Map<?, ?> settings) {
                        this((String) settings.get("function"), (String) settings.get("apply_if"));
                    }
                }
            }

            static FieldBoost load(Map<?, ?> settings) {
                Map<?, ?> fieldBoostSettings = getAsMap(settings, "field_boost");
                List<Field> fields = getAsStream(fieldBoostSettings, "fields")
                        .map(Map.class::cast)
                        .map(Field::new)
                        .toList();
                return new FieldBoost(fields,
                        (int) fieldBoostSettings.get("default_boost_factor"),
                        (int) fieldBoostSettings.get("phrase_boost_divisor"),
                        (String) fieldBoostSettings.get("multi_match_type"),
                        (Boolean) fieldBoostSettings.get("analyze_wildcard"),
                        (Boolean) fieldBoostSettings.get("include_exact_fields")
                );
            }
        }

        record FunctionScore(List<ScoreFunction> functions, String scoreMode, String boostMode) {
            sealed interface ScoreFunction permits ScoreFunction.FieldValueFactor {
                record FieldValueFactor(Map<?, ?> params, int weight) implements ScoreFunction {
                    @Override
                    public Map<String, Object> get() {
                        return Map.of(key(), params,
                                "weight", weight);
                    }

                    public static String key() {
                        return "field_value_factor";
                    }
                }

                Map<String, Object> get();

                static ScoreFunction load(Map<?, ?> settings) {
                    return FieldValueFactor.key().equals(settings.get("type"))
                            ? new FieldValueFactor(getAsMap(settings, "params"), (int) settings.get("weight"))
                            : null;
                }
            }

            static FunctionScore load(Map<?, ?> settings) {
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

        record ConstantScore(List<Constant> constant) {
            record Constant(List<Score> scores, Wrapper wrapper) {
                enum Wrapper {
                    none(List::getFirst),
                    should(QueryUtil::shouldWrap);

                    final Function<List<Map<String, Object>>, Map<String, Object>> wrap;

                    Wrapper(Function<List<Map<String, Object>>, Map<String, Object>> wrap) {
                        this.wrap = wrap;
                    }

                    static Wrapper load(Map<?, ?> settings) {
                        if (!settings.containsKey("wrapper")) {
                            return Wrapper.none;
                        }
                        try {
                            return Wrapper.valueOf((String) settings.get("wrapper"));
                        } catch (IllegalArgumentException e) {
                            return Wrapper.none;
                        }
                    }
                }

                record Score(String field, String value, int score) {
                    Score(Map<?, ?> settings) {
                        this((String) settings.get("field"), (String) settings.get("value"), (int) settings.get("score"));
                    }
                }

                static Constant load(Map<?, ?> settings) {
                    List<Score> scores = getAsStream(settings, "scores")
                            .map(Map.class::cast)
                            .map(Score::new)
                            .toList();
                    return new Constant(scores, Wrapper.load(settings));
                }
            }

            static ConstantScore load(Map<?, ?> settings) {
                List<ConstantScore.Constant> constants = getAsStream(settings, "constant_score")
                        .map(Map.class::cast)
                        .map(ConstantScore.Constant::load)
                        .toList();
                return new ConstantScore(constants);
            }
        }

        private static Stream<?> getAsStream(Map<?, ?> m, String k) {
            return m.containsKey(k) ? ((List<?>) m.get(k)).stream() : Stream.empty();
        }

        private static Map<?, ?> getAsMap(Map<?, ?> m, String k) {
            return  m.containsKey(k) ? (Map<?, ?>) m.get(k) : Map.of();
        }
    }

    private static Map<?, ?> toMap(InputStream json) {
        try {
            return mapper.readValue(json, Map.class);
        } catch (IOException ignored) {
            return Map.of();
        }
    }
}

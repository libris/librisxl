package whelk.search2;

import whelk.Whelk;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
        return Boost.load(settings);
    }

    public record Boost(FieldBoost fieldBoost, FunctionScore functionScore, ConstantScore constantScore) {
        record FieldBoost(List<Field> fields,
                          int defaultBoostFactor,
                          int phraseBoostDivisor,
                          String multiMatchType,
                          boolean analyzeWildcard,
                          boolean includeExactFields) {
            record Field(String name, int boost, ScriptScore scriptScore) {
                record ScriptScore(String function, String applyIf) {
                }
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
                            ? new FieldValueFactor((Map<?, ?>) settings.get("params"), (int) settings.get("weight"))
                            : null;
                }
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
        }

        private static Boost load(Map<?, ?> settings) {
            Map<?, ?> fieldBoostSettings = getAsMap(settings, "field_boost");
            List<FieldBoost.Field> fields = new ArrayList<>();
            getAsMap(fieldBoostSettings, "fields").forEach((k, v) -> {
                String field = (String) k;
                int boost = (int) ((Map<?, ?>) v).get("boost");
                Map<?, ?> scriptScore = getAsMap((Map<?, ?>) v, "script_score");
                if (scriptScore.isEmpty()) {
                    fields.add(new FieldBoost.Field(field, boost, null));
                } else {
                    String function = (String) scriptScore.get("function");
                    String applyIf = (String) scriptScore.get("apply_if");
                    fields.add(new FieldBoost.Field(field, boost, new FieldBoost.Field.ScriptScore(function, applyIf)));
                }
            });
            FieldBoost fieldBoost = new FieldBoost(fields,
                    (int) fieldBoostSettings.get("default_boost_factor"),
                    (int) fieldBoostSettings.get("phrase_boost_divisor"),
                    (String) fieldBoostSettings.get("multi_match_type"),
                    (Boolean) fieldBoostSettings.get("analyze_wildcard"),
                    (Boolean) fieldBoostSettings.get("include_exact_fields")
            );

            Map<?, ?> functionScoreSettings = getAsMap(settings, "function_score");
            List<FunctionScore.ScoreFunction> scoreFunctions = getAsStream(functionScoreSettings, "functions")
                    .map(Map.class::cast)
                    .map(FunctionScore.ScoreFunction::load)
                    .filter(Objects::nonNull)
                    .toList();
            FunctionScore functionScore = new FunctionScore(scoreFunctions,
                    (String) functionScoreSettings.get("score_mode"),
                    (String) functionScoreSettings.get("boost_mode"));


            List<ConstantScore.Constant> constants = getAsStream(settings, "constant_score")
                    .map(Map.class::cast)
                    .map(ConstantScore.Constant::load)
                    .toList();
            ConstantScore constantScore = new ConstantScore(constants);

            return new Boost(fieldBoost, functionScore, constantScore);
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

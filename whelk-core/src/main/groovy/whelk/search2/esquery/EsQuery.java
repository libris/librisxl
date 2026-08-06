package whelk.search2.esquery;

import whelk.search2.Operator;
import whelk.search2.Query;
import whelk.search2.QueryUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.isSimple;

public sealed interface EsQuery {
    enum MultiMatchType {
        // https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-multi-match-query#multi-match-types
        best_fields,
        most_fields
    }

    Map<String, Object> dsl();

    sealed interface TextQueryMode {
        String mode();
        String query();
        MultiMatchType multiMatchType();

        static TextQueryMode from(String s) {
            return isSimple(s)
                    ? new SimpleQueryString(s)
                    : new QueryString(QueryUtil.escapeNonSimpleQueryString(s), MultiMatchType.most_fields);
        }
    }

    record QueryString(String query, MultiMatchType multiMatchType) implements TextQueryMode {
        @Override
        public String mode() {
            return "query_string";
        }
    }

    record SimpleQueryString(String query) implements TextQueryMode {
        @Override
        public String mode() {
            return "simple_query_string";
        }

        @Override
        public MultiMatchType multiMatchType() {
            // FIXME: Rewrite comment
            // While the multi_match type may not be specified in simple_query_string,
            // the score counting corresponds to most_fields
            return MultiMatchType.most_fields;
        }
    }

    record TextQuery(TextQueryMode query,
                     List<EsBoost.Field> boostFields,
                     Query.Connective connective,
                     EsBoost.TextQuerySettings settings) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            var q = new HashMap<>();

            q.put("query", query.query());
            q.put("default_operator", connective.name());

            if (settings.analyzeWildcard()) {
                q.put("analyze_wildcard", true);
            }

            if (!boostFields.isEmpty()) {
                List<String> formattedFields = boostFields.stream()
                        .map(f -> formatBoostField(f.name(), f.boost()))
                        .collect(Collectors.toList());

                if (settings.includeExactFields()) {
                    q.put("quote_field_suffix", settings.quoteFieldSuffix());

                    boostFields.forEach(f -> {
                        // Exact fields are only relevant for scoring, no need to include non-scoring fields
                        if (f.boost() > 0) {
                            String exactField = f.name() + settings.quoteFieldSuffix();
                            formattedFields.add(formatBoostField(exactField, f.boost()));
                        }
                    });
                }

                if (query instanceof QueryString qs && boostFields.size() > 1) {
                    q.put("type", qs.multiMatchType().name());
                }

                q.put("fields", formattedFields);
            }

            return Map.of(query.mode(), q);
        }

        public TextQuery withFields(List<EsBoost.Field> newFields) {
            return new TextQuery(query, newFields, connective, settings);
        }

        public boolean isSimple() {
            return query instanceof SimpleQueryString;
        }

        public static TextQuery simpleUnboostedQuery(String query, String field) {
            return new TextQuery(new SimpleQueryString(query),
                    List.of(EsBoost.Field.unboosted(field)),
                    Query.Connective.AND,
                    EsBoost.FieldedQuerySettings.defaultSettings());
        }

        private String formatBoostField(String fieldName, float boost) {
            return boost == 1 ? fieldName : String.format("%s^%s", fieldName, boost);
        }
    }

    record TermQuery(String field, Object value) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("bool", Map.of("filter", Map.of("term", Map.of(field, value))));
        }
    }

    sealed interface Disjunction extends EsQuery {
        @Override
        Map<String, Object> dsl();

        List<EsQuery> subQueries();
        Disjunction withSubQueries(List<EsQuery> subQueries);
    }

    record Should(List<EsQuery> subQueries) implements Disjunction {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("bool", Map.of("should", subQueries.stream().map(EsQuery::dsl).toList()));
        }

        @Override
        public Should withSubQueries(List<EsQuery> subQueries) {
            return new Should(subQueries);
        }
    }

    record DisMax(List<EsQuery> subQueries) implements Disjunction {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("dis_max", Map.of("queries", subQueries.stream().map(EsQuery::dsl).toList()));
        }

        @Override
        public DisMax withSubQueries(List<EsQuery> subQueries) {
            return new DisMax(subQueries);
        }
    }

    record Must(List<EsQuery> subQueries) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("bool", Map.of("must", subQueries.stream().map(EsQuery::dsl).toList()));
        }
    }

    record MustNot(EsQuery query) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("bool", Map.of("must_not", query.dsl()));
        }
    }

    record Exists(String field) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("exists", Map.of("field", field));
        }
    }

    record Nested(EsQuery query, NestedStem stem, Set<NestedField> fields) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            if (fields.size() == 1 && stem.includeInParent()) {
                boolean isSingleTokenQuery = !(query instanceof TextQuery textQuery)
                        || textQuery.query().query().chars().noneMatch(Character::isWhitespace);
                if (isSingleTokenQuery) {
                    // Not necessary to wrap as nested query in this case
                    return query.dsl();
                }
            }
            return Map.of("nested", Map.of(
                    "ignore_unmapped", true, // otherwise can fail when searching multiple indices
                    "path", stem.stem(),
                    "query", query.dsl()
            ));
        }
    }

    record NestedStem(String stem, boolean includeInParent) {
    }

    record NestedField(String field, boolean isRepeatable) {
    }

    record MatchAll() implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("match_all", Map.of());
        }
    }

    record MatchNone() implements EsQuery {
        // FIXME: Handle queries that are syntactically correct but make no sense and are guaranteed to return no hits
        @Override
        public Map<String, Object> dsl() {
            return Map.of("exists", Map.of("field", "nonsense.field"));
        }
    }

    record RangeQuery(String field, Map<Operator, Object> range) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            Map<String, Object> m = new HashMap<>();
            range.forEach((op, v) -> {
                switch (op) {
                    case GREATER_THAN_OR_EQUALS -> m.put("gte", v);
                    case GREATER_THAN -> m.put("gt", v);
                    case LESS_THAN_OR_EQUALS -> m.put("lte", v);
                    case LESS_THAN -> m.put("lt", v);
                    default -> {}
                }
            });
            return Map.of("bool", Map.of("filter", Map.of("range", Map.of(field, m))));
        }
    }

    record ConstantScore(EsQuery query, float score) implements EsQuery {
        @Override
        public Map<String, Object> dsl() {
            return Map.of("constant_score", Map.of("filter", query.dsl(), "boost", score));
        }
    }

    sealed interface ScoreFunction extends EsQuery permits FieldValueFactor, ScriptScore {
        @Override
        Map<String, Object> dsl();
    }

    record FieldValueFactor(Map<?, ?> params, float weight) implements ScoreFunction {
        public static String key() {
            return "field_value_factor";
        }

        @Override
        public Map<String, Object> dsl() {
            return Map.of(key(), params,
                    "weight", weight);
        }
    }

    record ScriptScore(EsQuery query, Script script) implements ScoreFunction {
        public static String key() {
            return "script_score";
        }

        @Override
        public Map<String, Object> dsl() {
            return Map.of("script_score", Map.of(
                            "query", query.dsl(),
                            "script", script.asMap()));
        }
    }

    record Script(String source, Map<String, Object> params) {
        Map<String, Object> asMap() {
            Map<String, Object> m = new HashMap<>();
            m.put("source", source);
            if (!params.isEmpty()) {
                m.put("params", params);
            }
            return m;
        }
    }
}
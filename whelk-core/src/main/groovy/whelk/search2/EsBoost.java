package whelk.search2;

import whelk.JsonLd;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static whelk.JsonLd.CACHE_RECORD_TYPE;
import static whelk.JsonLd.RECORD_TYPE;
import static whelk.JsonLd.VIRTUAL_RECORD_TYPE;
import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.shouldWrap;

public class EsBoost {
    // TODO: Don't hardcode boost configuration
    public static List<String> BOOST_FIELDS = List.of(
            "_topChipStr^400(1 / doc['_topChipStr.length'].value)",
            "_topChipStr.exact^400(1 / doc['_topChipStr.length'].value)",
            "_chipStr^200",
            "_chipStr.exact^200",
            "_cardStr^50",
            "_cardStr.exact^50",
            "_searchCardStr^1",
            "_searchCardStr.exact^1"
    );

    public static List<ScoreFunction> SCORE_FUNCTIONS = List.of(
            new FieldValueFactor("reverseLinks.totalItemsByRelation.instanceOf", 10, "ln1p", 0, 15),
            new FieldValueFactor("reverseLinks.totalItemsByRelation.itemOf.instanceOf", 10, "ln1p", 0, 10)
//            new MatchingFieldValue("language.@id", "https://id.kb.se/language/swe", 50)
    );

    public static Map<String, Object> addBoosts(Map<String, Object> esQuery, List<ScoreFunction> scoreFunctions) {
        return mustWrap(List.of(esQuery, recordsOverCacheRecordsBoost(), functionScores(scoreFunctions)));
    }

    private static Map<String, Object> recordsOverCacheRecordsBoost() {
        var recordType = JsonLd.RECORD_KEY + '.' + JsonLd.TYPE_KEY;

        var recordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, RECORD_TYPE)),
                        "boost", 1000)
        );
        var virtualRecordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, VIRTUAL_RECORD_TYPE)),
                        "boost", 1000)
        );
        var cacheRecordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, CACHE_RECORD_TYPE)),
                        "boost", 1)
        );

        return shouldWrap(List.of(recordBoost, virtualRecordBoost, cacheRecordBoost));
    }

    private static Map<String, Object> functionScores(List<ScoreFunction> scoreFunctions) {
        List<Map<String, Object>> functions = (scoreFunctions.isEmpty() ? SCORE_FUNCTIONS : scoreFunctions).stream()
                .map(EsBoost.ScoreFunction::toEs)
                .toList();
        return Map.of("function_score",
                Map.of("query", Map.of("match_all", Map.of()),
                        "functions", functions,
                        "score_mode", "sum",
                        "boost_mode", "sum"));
    }

    public sealed interface ScoreFunction permits FieldValueFactor, MatchingFieldValue {
        Map<String, Object> toEs();
        List<String> paramList();
    }

    public record FieldValueFactor(String field, float factor, String modifier, float missing, float weight) implements ScoreFunction {
        @Override
        public Map<String, Object> toEs() {
            return Map.of(
                    "field_value_factor", Map.of(
                            "field", field,
                            "factor", factor,
                            "modifier", modifier,
                            "missing", missing),
                    "weight", weight);
        }

        @Override
        public List<String> paramList() {
            return List.of("fvf", field, Float.toString(factor), modifier, Float.toString(missing), Float.toString(weight));
        }
    }

    public record MatchingFieldValue(String field, String value, float boost) implements ScoreFunction {
        @Override
        public Map<String, Object> toEs() {
            String script = String.format(Locale.US, "doc['%s'].value == '%s' ? %.2f : 0", field, value, boost);
            return Map.of("script_score", Map.of("script", Map.of("source", script)));
        }

        @Override
        public List<String> paramList() {
            return List.of("mfv", field, value, Float.toString(boost));
        }
    }
}

package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.util.DocumentUtil;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Query.NESTED_AGG_NAME;
import static whelk.search2.QueryParams.ApiParams.PREDICATES;
import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.util.DocumentUtil.getAtPath;
import static whelk.util.DocumentUtil.traverse;

public class QueryResult {
    public record Aggregation(String property, String path, List<Bucket> buckets) {
    }

    public record Bucket(String value, int count, List<Aggregation> subAggregations) {
    }

    public final int numHits;
    public final List<Aggregation> aggs;
    public final List<Bucket> pAggs;
    public final List<Spell.Suggestion> spell;

    private final List<EsItem> esItems;
    private final List<String> debug;

    public QueryResult(Map<?, ?> esResponse, List<String> debug) {
        Map<String, Object> mainQueryResponse = getMainResponse(esResponse);
        this.debug = debug;
        this.numHits = getNumHits(mainQueryResponse);
        this.esItems = collectEsItems(mainQueryResponse);
        this.aggs = collectAggResult(getAggregations(mainQueryResponse));
        this.spell = Spell.collectSuggestions(mainQueryResponse);
        this.pAggs = collectPAggResult(getPAggregations(mainQueryResponse, getSecondaryResponse(esResponse)));
    }

    public List<Map<String, Object>> collectItems(Function<Map<String, Object>, Map<String, Object>> applyLens) {
        return esItems.stream().map(item -> item.toLd(applyLens)).toList();
    }

    private static int getNumHits(Map<String, Object> esResponse) {
        return (int) getAtPath(esResponse, List.of("hits", "total", "value"), 1);
    }

    private List<EsItem> collectEsItems(Map<String, Object> esResponse) {
        return ((List<?>) getAtPath(esResponse, List.of("hits", "hits"), Collections.emptyList()))
                .stream()
                .map(Map.class::cast)
                .map(hit -> {
                    var item = castToStringObjectMap(hit.get("_source"));
                    item.put("_id", hit.get("_id"));
                    if (debug.contains(QueryParams.Debug.ES_SCORE)) {
                        item.put("_score", hit.get("_score"));
                        item.put("_explanation", hit.get("_explanation"));
                        item.put("_fields", hit.get("fields"));
                    }
                    return item;
                })
                .map(EsItem::new)
                .toList();
    }

    private static List<Bucket> collectPAggResult(Map<String, Object> aggs) {
        return ((Map<?, ?>) aggs.getOrDefault("buckets", Map.of()))
                .entrySet()
                .stream()
                .map(e -> new Bucket((String) e.getKey(), (int) ((Map<?, ?>) e.getValue()).get("doc_count"), null))
                .toList();
    }

    public static List<Aggregation> collectAggResult(Map<String, Object> aggsMap) {
        var aggregations = new ArrayList<Aggregation>();

        for (var e : aggsMap.entrySet()) {
            var path = e.getKey();
            var aggs = (Map<?, ?>) e.getValue();
            if (path.equals(QueryParams.ApiParams.PREDICATES)) {
                continue;
            }
            var property = aggs
                    .keySet()
                    .stream()
                    .filter(Predicate.not("doc_count"::equals))
                    .map(String.class::cast)
                    .findFirst()
                    .get();

            if (!(aggs.get(property) instanceof Map<?, ?> agg))
                continue;

            if (agg.containsKey(NESTED_AGG_NAME)) {
                agg = (Map<?, ?>) agg.get(NESTED_AGG_NAME);
            }

            var buckets = ((List<?>) agg.get("buckets")).stream()
                    .map(Map.class::cast)
                    .map(b -> new Bucket(
                            String.valueOf(b.get("key")),
                            (Integer) b.get("doc_count"),
                            collectAggResult(aggsMap(b))))
                    .toList();

            aggregations.add(new Aggregation(property, path, buckets));
        }

        return aggregations;
    }

    private static Map<String, Object> aggsMap(Map<?, ?> bucketMap) {
        return castToStringObjectMap(bucketMap).entrySet().stream()
                .filter(e -> !"key".equals(e.getKey()))
                .filter(e -> !"doc_count".equals(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Object> getMainResponse(Map<?, ?> esResponse) {
        return normalizeResponse(esResponse.get("responses") instanceof List<?> l
                ? (Map<?, ?>) l.getFirst()
                : esResponse);
    }

    private static Map<String, Object> getSecondaryResponse(Map<?, ?> esResponse) {
        return normalizeResponse(esResponse.get("responses") instanceof List<?> l && l.size() > 1
                ? (Map<?, ?>) l.get(1)
                : Map.of());
    }

    private static Map<String, Object> getAggregations(Map<String, Object> esResponse) {
        var aggs = castToStringObjectMap(esResponse.get("aggregations"));
        aggs.remove(PREDICATES);
        return aggs;
    }

    private static Map<String, Object> getPAggregations(Map<String, Object> mainResponse, Map<String, Object> secondaryResponse) {
        var aggs = ((Map<?, ?>) (secondaryResponse.isEmpty() ? mainResponse : secondaryResponse)
                .getOrDefault("aggregations", Map.of()))
                .get(PREDICATES);
        return castToStringObjectMap(aggs);
    }

    private static Map<String, Object> normalizeResponse(Map<?, ?> esResponse) {
        var norm = new LinkedHashMap<String, Object>();
        esResponse.forEach((k, v) ->
                {
                    if (v != null) {
                        norm.put((String) k, v);
                    }
                }
        );
        return norm;
    }

    private class EsItem {
        private final Map<String, Object> map;

        EsItem(Map<String, Object> map) {
            this.map = map;
        }

        private Map<String, Object> toLd(Function<Map<String, Object>, Map<String, Object>> applyLens) {
            LdItem ldItem = new LdItem(applyLens.apply(map));

            // ISNIs and ORCIDs are indexed with and without spaces, remove the one with spaces.
            ldItem.normalizeIsniAndOrcid();
            // reverseLinks must be re-added because they might get filtered out in applyLens().
            var reverseLinks = getReverseLinks();
            if (!reverseLinks.isEmpty()) {
                ldItem.addReverseLinks(reverseLinks);
            }
            if (debug.contains(QueryParams.Debug.ES_SCORE)) {
                ldItem.addScore(getScoreExplanation(), getFields());
            }

            return ldItem.map;
        }

        private Map<String, Object> getReverseLinks() {
            return castToStringObjectMap(map.get("reverseLinks"));
        }

        private Map<String, Object> getScoreExplanation() {
            return castToStringObjectMap(map.get("_explanation"));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Map<String, Object> getFields() {
            return castToStringObjectMap(map.get("_fields")).entrySet().stream()
                    .flatMap(this::flattenNestedField)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> {
                        if (a instanceof List l1 && b instanceof List l2) {
                            l1.addAll(l2);
                        }
                        return a;
                    }));
        }

        private Stream<Map.Entry<String, Object>> flattenNestedField(Map.Entry<String, Object> entry) {
            var k = entry.getKey();
            var v = (List<?>) entry.getValue();
            if (v.stream().allMatch(Map.class::isInstance)) {
                return v.stream().map(QueryUtil::castToStringObjectMap)
                        .flatMap(m -> m.entrySet().stream().map(e -> Map.entry(k + "." + e.getKey(), e.getValue())))
                        .flatMap(this::flattenNestedField);
            }
            return Stream.of(entry);
        }
    }

    private static class LdItem {
        private final Map<String, Object> map;

        LdItem(Map<String, Object> map) {
            this.map = map;
        }

        private void normalizeIsniAndOrcid() {
            Function<Object, String> toStr = s -> s != null ? s.toString() : "";
            List<Map<String, Object>> identifiedBy = getIdentifiedBy();
            if (!identifiedBy.isEmpty()) {
                identifiedBy.removeIf(id -> (Document.isIsni(id) || Document.isOrcid(id))
                        && toStr.apply(id.get("value")).length() == 16 + 3);
                map.put("identifiedBy", identifiedBy);
            }
        }

        private List<Map<String, Object>> getIdentifiedBy() {
            return ((List<?>) map.getOrDefault("identifiedBy", Collections.emptyList()))
                    .stream()
                    .map(QueryUtil::castToStringObjectMap)
                    .collect(Collectors.toList());
        }

        private void addReverseLinks(Map<String, Object> reverseLinks) {
            reverseLinks.put(JsonLd.ID_KEY, makeFindOLink((String) map.get(JsonLd.ID_KEY)));
            map.put("reverseLinks", reverseLinks);
        }

        private void addScore(Map<String, Object> scoreExplanation, Map<String, Object> fields) {
            var scorePerField = getScorePerField(scoreExplanation);
            var totalScore = scorePerField.values().stream().reduce((double) 0, Double::sum);
            var matchedFields = scorePerField.keySet().stream()
                    .map(f -> f.split(":")[0])
                    .filter(fields::containsKey)
                    .collect(Collectors.toMap(Function.identity(),
                            fields::get,
                            (k1, k2) -> k1)
                    );
            var scoreData = Map.of("_total", totalScore, "_perField", scorePerField, "_matchedFields", matchedFields, "_explain", scoreExplanation);
            map.put("_debug", Map.of("_score", scoreData));
        }

        private static Map<String, Double> getScorePerField(Map<String, Object> scoreExplanation) {
            Map<String, Double> scorePerField = new HashMap<>();
            Map<String, Double> scriptScorePerField = new HashMap<>();
            List<Object> scriptScorePath = new ArrayList<>();
            AtomicReference<Double> scriptScore = new AtomicReference<>(0.0);
            traverse(scoreExplanation, (value, path) -> {
                if (!scriptScorePath.isEmpty() && (path.size() < scriptScorePath.size() || !path.subList(0, scriptScorePath.size()).equals(scriptScorePath))) {
                    Double factor = scriptScore.get() / scriptScorePerField.values().stream().reduce(0.0, Double::sum);
                    scriptScorePerField.forEach((field, score) -> scorePerField.put(field, score * factor));
                    scriptScorePerField.clear();
                    scriptScorePath.clear();
                    scriptScore.set(0.0);
                }
                if (value instanceof Map<?, ?> m) {
                    String description = (String) m.get("description");
                    if (description.contains("[PerFieldSimilarity]")) {
                        Double score = (Double) m.get("value");
                        if (score > 0) {
                            if (scriptScorePath.isEmpty()) {
                                scorePerField.put(parseField(description), score);
                            } else {
                                scriptScorePerField.put(parseField(description), score);
                            }
                        }
                    }
                    else if ("function score, score mode [sum]".equals(description)) {
                        ((List<?>) m.get("details")).stream()
                                .map(Map.class::cast)
                                .forEach(o -> {
                                    Double score = (Double) o.get("value");
                                    if (score > 0) {
                                        ((List<?>) o.get("details")).stream()
                                                .map(Map.class::cast)
                                                .map(_m -> (String) _m.get("description"))
                                                .filter(desc -> desc.startsWith("field value function:"))
                                                .map(LdItem::parseField)
                                                .findFirst()
                                                .ifPresent(field -> scorePerField.put(field, score));
                                    }
                                });
                    }
                    else if ("max of:".equals(description)) {
                        Double score = (Double) m.get("value");
                        if (score > 0) {
                            ((List<?>) m.get("details")).stream()
                                    .map(Map.class::cast)
                                    .filter(_m -> score.equals(_m.get("value")))
                                    .map(_m -> (String) _m.get("description"))
                                    .map(LdItem::parseField)
                                    .findFirst()
                                    .ifPresent(field -> scorePerField.put(field, score));
                        }
                    } else if (description.startsWith("script score function")) {
                        Double score = (Double) m.get("value");
                        if (score > 0) {
                            scriptScore.set(score);
                            scriptScorePath.addAll(path);
                        }
                    }
                }
                return new DocumentUtil.Nop();
            });

            return scorePerField.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> n, LinkedHashMap::new));
        }

        private static String parseField(String description) {
            if (description.startsWith("weight(")) {
                description = description.replace("weight(", "");
                if (description.startsWith("Synonym(")) {
                    description = description.replace("Synonym(", "");
                    Matcher matcher = Pattern.compile("^[^ ]+:[^ ]+( [^ ]+:[^ )]+)+").matcher(description);
                    if (matcher.find()) {
                        String match = matcher.group();
                        String key = match.substring(0, match.indexOf(":"));
                        String values = Arrays.stream(match.split(" "))
                                .map(s -> s.split(":"))
                                .map(s -> s[1])
                                .collect(Collectors.joining(" "));
                        return key + ":(" + values + ")";
                    }
                } else {
                    Matcher m = Pattern.compile("^[^ ]+:((\".+\")|[^ ]+)").matcher(description);
                    if (m.find()) {
                        return m.group();
                    }
                }
            } else if (description.startsWith("field value function:") || description.startsWith("script score function")) {
                Matcher matcher = Pattern.compile("doc\\['[^ ]+']").matcher(description);
                if (matcher.find()) {
                    String match = matcher.group();
                    String key = match.substring(match.indexOf("'") + 1);
                    return key.substring(0, key.indexOf("'")) + ":N/A";
                }
            }
            return Stream.of(description.split("\\^")).findFirst().get();
        }

        private static String makeFindOLink(String iri) {
            return Document.getBASE_URI()
                    .resolve("find?o=" + URLEncoder.encode(iri, StandardCharsets.UTF_8))
                    .toString();
        }
    }
}

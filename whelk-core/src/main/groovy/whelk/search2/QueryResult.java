package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.util.DocumentUtil;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static whelk.JsonLd.SEARCH_KEY;
import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.util.DocumentUtil.getAtPath;
import static whelk.util.DocumentUtil.traverse;

public class QueryResult {
    public final int numHits;
    public final List<Aggs.Aggregation> aggs;
    public final List<Aggs.Bucket> pAggs;
    public final List<Spell.Suggestion> spell;

    private final List<EsItem> esItems;
    private final List<String> debug;

    public QueryResult(Map<?, ?> esResponse, List<String> debug) {
        var normResponse = normalizeResponse(esResponse);
        this.debug = debug;
        this.numHits = getNumHits(normResponse);
        this.esItems = collectEsItems(normResponse);
        this.aggs = Aggs.collectAggResult(normResponse);
        this.pAggs = Aggs.collectPAggResult(normResponse);
        this.spell = Spell.collectSuggestions(normResponse);
    }

    public QueryResult(Map<?, ?> esResponse) {
        this(esResponse, List.of());
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
                    }
                    return item;
                })
                .map(EsItem::new)
                .toList();
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
            getReverseLinks().ifPresent(ldItem::addReverseLinks);

            if (debug.contains(QueryParams.Debug.ES_SCORE)) {
                ldItem.addSearchStrings(this);
                getScoreExplanation().ifPresent(ldItem::addScore);
            }

            return ldItem.map;
        }

        private Optional<Map<String, Object>> getReverseLinks() {
            return Optional.ofNullable(map.get("reverseLinks"))
                    .map(QueryUtil::castToStringObjectMap);
        }

        private Optional<Map<String, Object>> getScoreExplanation() {
            return Optional.ofNullable(map.get("_explanation"))
                    .map(QueryUtil::castToStringObjectMap);
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

        @SuppressWarnings({"rawtypes", "unchecked"})
        private void addSearchStrings(EsItem esItem) {
            DocumentUtil.traverse(esItem.map, (value, path) -> {
                if (!path.isEmpty() && path.getLast().equals(SEARCH_KEY)) {
                    if (getAtPath(map, path.subList(0, path.size() - 1)) instanceof Map m) {
                        m.put(SEARCH_KEY, value);
                    }
                }
                return new DocumentUtil.Nop();
            });
        }

        private void addScore(Map<String, Object> scoreExplanation) {
            var scorePerField = getScorePerField(scoreExplanation);
            var totalScore = scorePerField.values().stream().reduce((double) 0, Double::sum);
            var scoreData = Map.of("_total", totalScore, "_perField", scorePerField, "_explain", scoreExplanation);
            map.put("_debug", Map.of("_score", scoreData));
        }

        private static Map<String, Double> getScorePerField(Map<String, Object> scoreExplanation) {
            Map<String, Double> scorePerField = new HashMap<>();

            traverse(scoreExplanation, (value, path) -> {
                if (value instanceof Map<?, ?> m) {
                    String description = (String) m.get("description");
                    if (description.contains("[PerFieldSimilarity]")) {
                        Double score = (Double) m.get("value");
                        if (score > 0) {
                            scorePerField.put(parseField(description), score);
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
            Matcher m = Pattern.compile("^weight\\(.+:((\".+\")|[^ ]+)").matcher(description);
            if (m.find()) {
                return m.group().replace("weight(", "");
            }
            return description;
        }

        private static String makeFindOLink(String iri) {
            return Document.getBASE_URI()
                    .resolve("find?o=" + URLEncoder.encode(iri, StandardCharsets.UTF_8))
                    .toString();
        }
    }
}

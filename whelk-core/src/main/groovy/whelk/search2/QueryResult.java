package whelk.search2;

import whelk.Document;
import whelk.JsonLd;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.util.DocumentUtil.getAtPath;

public class QueryResult {
    public final int numHits;
    private final List<EsItem> esItems;
    public final List<Aggs.Aggregation> aggs;
    public final List<Aggs.Bucket> pAggs;
    public final List<Spell.Suggestion> spell;
    public final List<Map<String, Object>> scores;

    public QueryResult(Map<?, ?> esResponse) {
        var normResponse = normalizeResponse(esResponse);
        this.numHits = getNumHits(normResponse);
        this.esItems = collectEsItems(normResponse);
        this.aggs = Aggs.collectAggResult(normResponse);
        this.pAggs = Aggs.collectPAggResult(normResponse);
        this.spell = Spell.collectSuggestions(normResponse);
        this.scores = collectScores(normResponse);
    }

    public List<Map<String, Object>> collectItems(Function<Map<String, Object>, Map<String, Object>> applyLens) {
        return esItems.stream().map(item -> item.toLd(applyLens)).toList();
    }

    private static int getNumHits(Map<String, Object> esResponse) {
        return (int) getAtPath(esResponse, List.of("hits", "total", "value"), 1);
    }

    private static List<EsItem> collectEsItems(Map<String, Object> esResponse) {
        return ((List<?>) getAtPath(esResponse, List.of("hits", "hits"), Collections.emptyList()))
                .stream()
                .map(Map.class::cast)
                .map(hit -> {
                    var item = castToStringObjectMap(hit.get("_source"));
                    item.put("_id", hit.get("_id"));
                    return item;
                })
                .map(EsItem::new)
                .toList();
    }

    private static List<Map<String, Object>> collectScores(Map<String, Object> esResponse) {
        return ((List<?>) getAtPath(esResponse, List.of("hits", "hits"), Collections.emptyList()))
                .stream()
                .filter(m -> ((Map<?, ?>) m).get("_score") != null)
                .map(QueryUtil::castToStringObjectMap)
                .filter(m -> m.keySet().retainAll(List.of("_id", "_score", "_explanation")))
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

    static class EsItem {
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
            return ldItem.map;
        }

        private Optional<Map<String, Object>> getReverseLinks() {
            return Optional.ofNullable(map.get("reverseLinks"))
                    .map(QueryUtil::castToStringObjectMap);
        }
    }

    static class LdItem {
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

        private static String makeFindOLink(String iri) {
            return Document.getBASE_URI()
                    .resolve("find?o=" + URLEncoder.encode(iri, StandardCharsets.UTF_8))
                    .toString();
        }
    }
}

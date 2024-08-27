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

public class QueryResult {
    public final int numHits;
    private final List<EsItem> esItems;
    public final List<Aggs.Aggregation> aggs;
    public final List<Aggs.Bucket> pAggs;
    public final List<Spell.Suggestion> spell;

    public QueryResult(Map<?, ?> esResponse) {
        var normResponse = normalizeResponse(esResponse);
        this.numHits = (int) normResponse.getOrDefault("totalHits", 0);
        this.esItems = getEsItems(normResponse);
        this.aggs = Aggs.collectAggResult(normResponse);
        this.pAggs = Aggs.collectPAggResult(normResponse);
        this.spell = Spell.collectSuggestions(normResponse);
    }

    public List<Map<String, Object>> collectItems(Function<Map<String, Object>, Map<String, Object>> applyLens) {
        return esItems.stream().map(item -> item.toLd(applyLens)).toList();
    }

    private static List<EsItem> getEsItems(Map<String, Object> esResponse) {
        return getAsList(esResponse, "items")
                .stream()
                .map(QueryUtil::castToStringObjectMap)
                .map(EsItem::new)
                .toList();
    }

    private static List<?> getAsList(Map<String, Object> m, String key) {
        return ((List<?>) m.getOrDefault(key, Collections.emptyList()));
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
            return getAsList(map, "identifiedBy")
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

package whelk.rest.api;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import groovy.lang.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.exception.WhelkRuntimeException;
import whelk.search.ESQuery;
import whelk.search.ElasticFind;
import whelk.search.RangeParameterPrefix;
import whelk.util.DocumentUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static whelk.search.ESQuery.Connective.AND;
import static whelk.search.ESQuery.Connective.OR;
import static whelk.util.Unicode.stripPrefix;

public class SearchUtils {
    private static final Logger log = LogManager.getLogger(SearchUtils.class);

    public static final int DEFAULT_LIMIT = 200;
    public static final int MAX_LIMIT = 4000;
    public static final int DEFAULT_OFFSET = 0;

    public static final String MATCHES_PROP = "matchesTransitive";

    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    public enum SearchType {
        ELASTIC,
        POSTGRES
    }

    Whelk whelk;
    JsonLd ld;
    ESQuery esQuery;
    URI vocabUri;

    public SearchUtils(Whelk whelk) {
        this(whelk.jsonld);
        this.whelk = whelk;
        this.esQuery = new ESQuery(whelk);
    }

    public SearchUtils(JsonLd jsonld) {
        this.ld = jsonld;
        if (ld.getVocabId() != null) {
            try {
                vocabUri = new URI(ld.getVocabId());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, Object> doSearch(Map<String, Object> queryParameters) throws InvalidQueryException, IOException {
        if (whelk.elastic == null) {
            throw new WhelkRuntimeException("ElasticSearch not configured.");
        }

        List<?> predicates = (List<?>) queryParameters.get("p");
        String object = getReservedQueryParameter("o", queryParameters);
        String value = getReservedQueryParameter("value", queryParameters);
        String query = getReservedQueryParameter("q", queryParameters);
        String sortBy = getReservedQueryParameter("_sort", queryParameters);
        String lens = getReservedQueryParameter("_lens", queryParameters);
        String addStats = getReservedQueryParameter("_stats", queryParameters);
        String suggest = getReservedQueryParameter("_suggest", queryParameters);
        String spell = getReservedQueryParameter("_spell", queryParameters);
        String computedLabel = getReservedQueryParameter(JsonLd.Platform.COMPUTED_LABEL, queryParameters);

        if (queryParameters.containsKey("p") && object == null) {
            throw new InvalidQueryException("Parameter 'p' can only be used together with 'o'");
        }

        if (suggest != null && !"chips".equals(lens)) {
            throw new InvalidQueryException("Parameter '_suggest' can only be used when '_lens' is set to 'chips'");
        }

        Tuple2<Integer, Integer> limitAndOffset = getLimitAndOffset(queryParameters);
        int limit = limitAndOffset.getV1();
        int offset = limitAndOffset.getV2();

        Map<String, Object> pageParams = new LinkedHashMap<>();
        pageParams.put("p", predicates);
        pageParams.put("value", value);
        pageParams.put("q", query);
        pageParams.put("o", object);
        pageParams.put("_sort", sortBy);
        pageParams.put("_limit", limit);
        pageParams.put("_lens", lens);
        pageParams.put("_stats", addStats);
        pageParams.put("_suggest", suggest);
        pageParams.put("_spell", spell);
        pageParams.put(JsonLd.Platform.COMPUTED_LABEL, computedLabel);

        return queryElasticSearch(
                queryParameters,
                pageParams,
                limit,
                offset,
                lens);
    }

    private Map<String, Object> applyLens(Map<String, Object> framedThing, String lens, String preserveId) {
        Set<String> preserveLinks = preserveId != null ? Collections.singleton(preserveId) : Collections.emptySet();

        return switch (lens != null ? lens : "") {
            case "chips" -> (Map<String, Object>) ld.toChip(framedThing, preserveLinks);
            case "full" -> removeSystemInternalProperties(framedThing);
            default -> (Map<String, Object>) ld.toCard(framedThing, false, false, false, preserveLinks, true);
        };
    }

    private Map<String, Object> queryElasticSearch(Map<String, Object> queryParameters,
                                                    Map<String, Object> pageParams,
                                                    int limit,
                                                    int offset,
                                                    String lens) throws InvalidQueryException {
        String query = (String) pageParams.get("q");
        String reverseObject = (String) pageParams.get("o");
        List<String> predicates = (List<String>) pageParams.get("p");
        String addStats = (String) pageParams.get("_stats");
        String suggest = (String) pageParams.get("_suggest");
        String spell = (String) pageParams.get("_spell");
        lens = lens != null ? lens : "cards";

        log.debug("Querying ElasticSearch");

        // SearchUtils may overwrite the `_limit` query param, and since it's
        // used for the other searches we overwrite limit here, so we keep it
        // consistent across search paths
        //
        // TODO Only manipulate `_limit` in one place
        queryParameters.put("_limit", List.of(String.valueOf(limit)));

        Map<String, Object> esResult = (Map<String, Object>) esQuery.doQuery((Map) queryParameters, suggest, spell);
        Lookup lookup = new Lookup();

        List<Map<String, Object>> mappings = new ArrayList<>();
        if (query != null) {
            Map<String, Object> mapping = new HashMap<>();
            mapping.put("variable", "q");
            mapping.put("predicate", lookup.chip("textQuery"));
            mapping.put("value", query);
            mappings.add(mapping);
        }

        Set<String> multiSelectable = ESQuery.multiSelectFacets(queryParameters);
        Tuple2<List<Map<String, Object>>, Map<String, Object>> mappingsAndPageParams = mapParams(lookup, queryParameters, multiSelectable);
        mappings.addAll(mappingsAndPageParams.getV1());
        pageParams.putAll(mappingsAndPageParams.getV2());

        int total = 0;
        if (esResult.get("totalHits") != null) {
            total = (Integer) esResult.get("totalHits");
        }

        List<Map<String, Object>> items = new ArrayList<>();
        if (esResult.get("items") != null) {
            List<Map<String, Object>> esItems = (List<Map<String, Object>>) esResult.get("items");
            for (Map<String, Object> it : esItems) {
                Map<String, Object> item = applyLens(it, lens, reverseObject);

                // ISNIs and ORCIDs are indexed with and without spaces, remove the one with spaces.
                List<Map<String, Object>> identifiedBy = (List<Map<String, Object>>) item.get("identifiedBy");
                if (identifiedBy != null) {
                    identifiedBy.removeIf(id ->
                        (Document.isIsni(id) || Document.isOrcid(id)) &&
                        id.get("value") != null &&
                        ((String) id.get("value")).length() == 16 + 3
                    );
                }

                // This object must be re-added because it might get filtered out in applyLens().
                item.put("reverseLinks", it.get("reverseLinks"));
                if (item.get("reverseLinks") != null) {
                    String encodedId = URLEncoder.encode((String) it.get("@id"), StandardCharsets.UTF_8);
                    String reverseLinksUrl = Document.getBASE_URI().resolve("find?o=" + encodedId).toString();
                    ((Map<String, Object>) item.get("reverseLinks")).put(JsonLd.ID_KEY, reverseLinksUrl);
                }
                items.add(item);
            }
        }

        Map<String, Object> aggregations = (Map<String, Object>) esResult.get("aggregations");
        Map<String, ?> selectedFacets = mappingsAndPageParams.getV2();

        // Filter out already selected facets (if not in multi-select group)
        for (Map.Entry<String, ?> entry : selectedFacets.entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            if (multiSelectable.contains(k)) {
                continue;
            }
            k = stripPrefix(k, ESQuery.AND_PREFIX);
            k = stripPrefix(k, ESQuery.OR_PREFIX);
            Map<String, Object> aggForKey = (Map<String, Object>) aggregations.get(k);
            if (aggForKey != null) {
                List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggForKey.get("buckets");
                if (buckets != null) {
                    buckets.removeIf(bucket -> {
                        Object bucketKey = bucket.get("key");
                        if (v instanceof List) {
                            return ((List<?>) v).contains(bucketKey);
                        }
                        return bucketKey.equals(v);
                    });
                }
            }
        }

        Map<String, Object> stats = null;
        if ((addStats == null || "true".equals(addStats) || "on".equals(addStats)) && !"only".equals(spell)) {
            Map<String, List<String>> multiSelectedMap = new HashMap<>();
            for (String key : multiSelectable) {
                Object value = selectedFacets.get(key);
                multiSelectedMap.put(key, value != null ? (List<String>) value : new ArrayList<>());
            }
            stats = buildStats(lookup, aggregations,
                               makeFindUrl(SearchType.ELASTIC, stripNonStatsParams(pageParams)),
                               (total > 0 && predicates == null) ? reverseObject : null,
                               multiSelectedMap);
        }
        if (stats == null) {
            log.debug("No stats found for query: {}", queryParameters);
        }

        List<Map<String, Object>> mappingsToProcess = query != null ? mappings.subList(1, mappings.size()) : mappings;
        for (Map<String, Object> mapping : mappingsToProcess) {
            Map<String, Object> params = removeMappingFromParams(pageParams, mapping);
            String upUrl = makeFindUrl(SearchType.ELASTIC, params, offset);
            mapping.put("up", Map.of(JsonLd.ID_KEY, upUrl));
        }

        if (reverseObject != null) {
            Map<String, Object> paramsWithoutO = new HashMap<>(pageParams);
            paramsWithoutO.remove("o");
            paramsWithoutO.remove("p");
            String upUrl = makeFindUrl(SearchType.ELASTIC, paramsWithoutO, offset);

            mappings.add(Map.of(
                    "variable", "o",
                    "object", lookup.chip(reverseObject), // TODO: object/predicate/???
                    "up", Map.of(JsonLd.ID_KEY, upUrl)
            ));
        }

        if (reverseObject != null && predicates != null && !predicates.isEmpty()) {
            Map<String, Object> paramsWithoutP = new HashMap<>(pageParams);
            paramsWithoutP.remove("p");
            String upUrl = makeFindUrl(SearchType.ELASTIC, paramsWithoutP, offset);
            mappings.add(Map.of(
                    "variable", "p",
                    "object", reverseObject,
                    "predicate", lookup.chip(predicates.getFirst()),
                    "up", Map.of(JsonLd.ID_KEY, upUrl)
            ));
        }

        Map<String, Object> result = assembleSearchResults(SearchType.ELASTIC,
                                                            items, mappings, pageParams,
                                                            limit, offset, total);

        if (stats != null && (suggest == null || !spell.equals("only"))) {
            stats.put(JsonLd.ID_KEY, "#stats");
            result.put("stats", stats);
        }

        if (spell != null) {
            List<Map<String, Object>> spellList = new ArrayList<>();
            List<Map<String, Object>> spellSuggestions = (List<Map<String, Object>>) esResult.get("spell");
            if (spellSuggestions != null) {
                for (Map<String, Object> suggestion : spellSuggestions) {
                    Map<String, Object> paramsWithNewQ = new HashMap<>(pageParams);
                    paramsWithNewQ.put("q", suggestion.get("text"));
                    spellList.add(Map.of(
                            "label", suggestion.get("text"),
                            "labelHTML", suggestion.get("highlighted"),
                            "view", Map.of(JsonLd.ID_KEY, makeFindUrl(SearchType.ELASTIC, paramsWithNewQ, offset))
                    ));
                }
            }
            result.put("_spell", spellList);
        }

        if (esResult.get("_debug") != null) {
            result.put("_debug", esResult.get("_debug"));
        }

        result.put("maxItems", String.valueOf(esQuery.getMaxItems()));

        lookup.run();

        return result;
    }

    Map<String, Object> removeMappingFromParams(Map<String, Object> pageParams, Map<String, Object> mapping) {
        Map<String, Object> params = new HashMap<>(pageParams);
        String variable = (String) mapping.get("variable");
        Object param = params.get(variable);
        List<Object> values = new ArrayList<>();
        if (param instanceof List) {
            values.addAll((List<?>) param);
        } else if (param != null) {
            values.add(param);
        }

        if (mapping.containsKey("object")) {
            Map<String, Object> object = (Map<String, Object>) mapping.get("object");
            Object value = object.get(JsonLd.ID_KEY);
            values.remove(value);
        } else if (mapping.containsKey("value")) {
            Object value = mapping.get("value");
            values.remove(value);
        }

        if (values.isEmpty()) {
            params.remove(variable);
        } else {
            params.put(variable, values);
        }
        return params;
    }

    Map<String, Object> removeSystemInternalProperties(Map<String, Object> framedThing) {
        DocumentUtil.traverse(framedThing, (value, path) -> {
            if (path != null && !path.isEmpty() && ((String) path.getLast()).startsWith("_")) {
                return new DocumentUtil.Remove();
            }
            return null;
        });
        return framedThing;
    }

    /*
     * Return a map without helper params, useful for facet links.
     */
    private Map<String, Object> stripNonStatsParams(Map<String, Object> incoming) {
        Map<String, Object> result = new HashMap<>();
        List<String> reserved = List.of("_offset");
        for (Map.Entry<String, Object> entry : incoming.entrySet()) {
            if (!reserved.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, Object> assembleSearchResults(SearchType st, List<Map<String, Object>> items,
                                                       List<Map<String, Object>> mappings, Map<String, Object> pageParams,
                                                       int limit, int offset, int total) throws InvalidQueryException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(JsonLd.TYPE_KEY, "PartialCollectionView");
        result.put(JsonLd.ID_KEY, makeFindUrl(st, pageParams, offset));
        result.put("itemOffset", offset);
        result.put("itemsPerPage", limit);
        result.put("totalItems", total);

        Map<String, Object> search = new HashMap<>();
        search.put("mapping", mappings);
        result.put("search", search);

        Map<String, Object> paginationLinks = makePaginationLinks(st, pageParams, limit,
                                                                   offset, Math.min(total, esQuery.getMaxItems()));
        result.putAll(paginationLinks);

        result.put("items", items);

        return result;
    }

    /**
     * Create ES filter for specified siteBaseUri.
     *
     */
    public Map<String, Object> makeSiteFilter(String siteBaseUri) {
        return Map.of(
                "should", Map.of(
                        "prefix", Map.of(JsonLd.ID_KEY, siteBaseUri),
                        // ideally, we'd use ID_KEY here too, but that
                        // breaks the test case :/
                        "prefix", Map.of("sameAs.@id", siteBaseUri)
                ),
                "minimum_should_match", 1
        );
    }

    /**
     * Build the term aggregation part of an ES query.
     *
     */
    public Map<String, Object> buildAggQuery(Object tree) {
        return buildAggQuery(tree, 10);
    }

    public Map<String, Object> buildAggQuery(Object tree, int size) {
        Map<String, Object> query = new HashMap<>();
        List<String> keys = new ArrayList<>();

        // In Python, the syntax for iterating over each item in a
        // list and for iterating over each key in a dict is the
        // same. That's not the case for Groovy, hence the following
        if (tree instanceof Map) {
            keys = new ArrayList<>(((Map<String, ?>) tree).keySet());
        } else if (tree instanceof List) {
            keys = (List<String>) tree;
        }

        for (String key : keys) {
            Map<String, Object> treeValue = null;
            if (tree instanceof Map) {
                Object value = ((Map<?, ?>) tree).get(key);
                if (value instanceof Map) {
                    treeValue = (Map<String, Object>) value;
                }
                // If value is a List or null, treeValue remains null
            }

            String sort = treeValue != null && "key".equals(treeValue.get("sort")) ? "_key" : "_count";
            String sortOrder = treeValue != null && "asc".equals(treeValue.get("sortOrder")) ? "asc" : "desc";

            Map<String, Object> terms = new HashMap<>();
            terms.put("field", key);
            terms.put("size", treeValue != null && treeValue.get("size") != null ? treeValue.get("size") : size);
            terms.put("order", Collections.singletonMap(sort, sortOrder));

            Map<String, Object> keyQuery = new HashMap<>();
            keyQuery.put("terms", terms);

            if (treeValue != null && treeValue.get("subItems") instanceof Map) {
                keyQuery.put("aggs", buildAggQuery(treeValue.get("subItems"), size));
            }

            query.put(key, keyQuery);
        }
        return query;
    }

    /*
     * Build aggregation statistics for ES result.
     *
     */
    private Map<String, Object> buildStats(Lookup lookup, Map<String, Object> aggregations, String baseUrl, String reverseObject, Map<String, List<String>> multiSelected) {
        return addSlices(lookup, new HashMap<>(), aggregations, baseUrl, reverseObject, multiSelected);
    }

    private Map<String, Object> addSlices(Lookup lookup, Map<String, Object> stats, Map<String, Object> aggregations, String baseUrl, String reverseObject, Map<String, List<String>> multiSelected) {
        Map<String, Object> sliceMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : aggregations.entrySet()) {
            String key = entry.getKey();
            Map<String, Object> aggregation = (Map<String, Object>) entry.getValue();

            String baseUrlForKey = removeWildcardForKey(baseUrl, key);
            List<Map<String, Object>> observations = new ArrayList<>();
            Map<String, Object> sliceNode = new HashMap<>();
            sliceNode.put("dimension", key);
            sliceNode.put("dimensionChain", makeDimensionChain(key));

            List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggregation.get("buckets");
            if (buckets != null) {
                for (Map<String, Object> bucket : buckets) {
                    String itemId = (String) bucket.get("key");

                    if (multiSelected.containsKey(key)) {
                        String param = makeParam(key, itemId);
                        boolean isSelected = multiSelected.get(key).contains(escapeQueryParam(itemId));
                        String searchPageUrl = isSelected
                                ? baseUrlForKey.replace("&" + param, "") // FIXME: generate up-link in a cleaner way
                                : baseUrlForKey + "&" + param;

                        Map<String, Object> observation = new HashMap<>();
                        observation.put("totalItems", bucket.get("doc_count"));
                        observation.put("view", Collections.singletonMap(JsonLd.ID_KEY, searchPageUrl));
                        observation.put("_selected", isSelected);
                        observation.put("object", lookup.chip(itemId));

                        observations.add(observation);
                    } else {
                        String searchPageUrl = baseUrlForKey + "&" + ESQuery.AND_PREFIX + makeParam(key, itemId);

                        Map<String, Object> observation = new HashMap<>();
                        observation.put("totalItems", bucket.get("doc_count"));
                        observation.put("view", Collections.singletonMap(JsonLd.ID_KEY, searchPageUrl));
                        observation.put("object", lookup.chip(itemId));

                        observations.add(observation);
                    }
                }
            }

            if (!observations.isEmpty()) {
                sliceNode.put("observation", observations);
                sliceMap.put(key, sliceNode);
            }
        }

        if (reverseObject != null && !hasHugeNumberOfIncomingLinks(reverseObject)) {
            List<Tuple2<List<String>, Long>> counts = groupRelations(whelk.getRelations().getReverseCountByRelation(reverseObject));  // TODO precompute and store in ES indexed
            List<Map<String, Object>> observations = new ArrayList<>();
            for (Tuple2<List<String>, Long> tuple : counts) {
                List<String> relations = tuple.getV1();
                long count = tuple.getV2();

                String viewUrl = baseUrl + "&" +
                        relations.stream()
                                .map(rel -> makeParam("p", rel + "." + JsonLd.ID_KEY))
                                .collect(Collectors.joining("&"));

                Map<String, Object> observation = new HashMap<>();
                observation.put("totalItems", count);
                observation.put("view", Collections.singletonMap("@id", viewUrl));
                observation.put("object", lookup.chip(relations.getFirst()));
                observations.add(observation);
            }

            Map<String, Object> sliceNode = new HashMap<>();
            sliceNode.put("dimension", JsonLd.REVERSE_KEY);
            sliceNode.put("observation", observations);
            sliceMap.put(JsonLd.REVERSE_KEY, sliceNode);
        }

        if (!sliceMap.isEmpty()) {
            stats.put("sliceByDimension", sliceMap);
        }

        return stats;
    }

    List<Object> makeDimensionChain(String key) {
        List<Object> dimensionChain = new ArrayList<>();
        for (String part : key.split("\\.")) {
            if (JsonLd.TYPE_KEY.equals(part)) {
                dimensionChain.add("rdf:type");
            } else if (!JsonLd.ID_KEY.equals(part)) {
                dimensionChain.add(part);
            }
        }

        if (!dimensionChain.isEmpty() && JsonLd.REVERSE_KEY.equals(dimensionChain.getFirst())) {
            dimensionChain.removeFirst();
            String inv = (String) dimensionChain.removeFirst();
            Map<String, Object> inverseMap = new HashMap<>();
            inverseMap.put("inverseOfTerm", inv);
            dimensionChain.addFirst(inverseMap);
        }
        return dimensionChain;
    }

    /**
     * Group together instanceOf.x and x
     *
     * Meaning there will be one predicate/facet for e.g. 'subject' and 'instanceOf.subject' called 'subject'.
     * That is, it will match both works and instances with local works.
     * Calling the relation 'subject' is of course not completely correct (it hides instanceOf) but the idea is that
     * it is more practical for now.
     */
    public static List<Tuple2<List<String>, Long>> groupRelations(Map<String, Long> counts) {
        Map<String, Long> blankWork = new HashMap<>();
        Map<String, Long> other = new HashMap<>();

        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String relation = entry.getKey();
            Long count = entry.getValue();
            if (relation.startsWith(JsonLd.WORK_KEY + ".")) {
                blankWork.put(relation, count);
            } else {
                other.put(relation, count);
            }
        }

        List<Tuple2<List<String>, Long>> result = new ArrayList<>();
        for (Map.Entry<String, Long> entry : other.entrySet()) {
            String relation = entry.getKey();
            Long count = entry.getValue();
            String r = JsonLd.WORK_KEY + "." + relation;
            if (blankWork.containsKey(r)) {
                List<String> relations = Arrays.asList(relation, r);
                result.add(new Tuple2<>(relations, count + blankWork.remove(r)));
            } else {
                result.add(new Tuple2<>(Collections.singletonList(relation), count));
            }
        }

        for (Map.Entry<String, Long> entry : blankWork.entrySet()) {
            String relation = entry.getKey();
            Long count = entry.getValue();
            String stripped = stripPrefix(relation, JsonLd.WORK_KEY + ".");
            result.add(new Tuple2<>(Arrays.asList(stripped, relation), count));
        }

        return result;
    }

    private String removeWildcardForKey(String url, String key) {
        return url.replace("&" + makeParam(key, "*"), "");
    }

    private boolean hasHugeNumberOfIncomingLinks(String iri) {
        int num = numberOfIncomingLinks(iri);
        return num < 0 || num > 500_000;
    }

    private int numberOfIncomingLinks(String iri) {
        try {
            Map<String, List<String>> queryMap = Collections.singletonMap(JsonLd.ID_KEY, Collections.singletonList(iri));
            Iterable<Map> results = new ElasticFind(esQuery).find(queryMap);
            List<Map<String, Object>> resultList = new ArrayList<>();
            for (Map m : results) {
                resultList.add((Map<String, Object>) m);
            }
            if (!resultList.isEmpty()) {
                Map<String, Object> doc = resultList.getFirst();
                Map<String, Object> reverseLinks = (Map<String, Object>) doc.get("reverseLinks");
                if (reverseLinks != null) {
                    Object totalItems = reverseLinks.get("totalItems");
                    if (totalItems instanceof Number) {
                        return ((Number) totalItems).intValue();
                    }
                }
            }
            return -1;
        } catch (Exception e) {
            log.warn("Error getting numberOfIncomingLinks for {}: {}", iri, e, e);
            return -1;
        }
    }

    private class Lookup {
        private final Multimap<String, Map<String, Object>> iriPos = ArrayListMultimap.create();

        Map<String, Object> chip(String itemRepr) {
            boolean matchesTerm = false;
            String itemId = itemRepr;
            String matchesPrefix = RangeParameterPrefix.MATCHES.prefix();
            if (itemRepr.startsWith(matchesPrefix)) {
                matchesTerm = true;
                itemId = itemId.substring(matchesPrefix.length());
            }

            String termKey = ld.toTermKey(itemId);
            if (ld.vocabIndex.containsKey(termKey)) {
                return (Map<String, Object>) ld.vocabIndex.get(termKey);
            }

            if (!itemId.startsWith("http") && itemId.contains(".")) {
                String[] parts = itemId.split("\\.");
                List<Map<String, Object>> chain = new ArrayList<>();
                for (String part : parts) {
                    if (!JsonLd.ID_KEY.equals(part)) {
                        chain.add(Lookup.this.chip(part));
                    }
                }
                String label = String.join(" ", parts);

                if (matchesTerm) {
                    Object proptype = chain.getLast().get(JsonLd.TYPE_KEY);
                    List<String> proptypes;
                    if (proptype instanceof String) {
                        proptypes = Collections.singletonList((String) proptype);
                    } else {
                        proptypes = (List<String>) proptype;
                    }
                    boolean anyObjectProperty = proptypes.stream().anyMatch(pt -> ld.isSubClassOf(pt, "ObjectProperty"));
                    if (anyObjectProperty) {
                        chain.add(Lookup.this.chip(MATCHES_PROP));
                        label = "matches " + label;
                    }
                }

                Map<String, Object> result = new HashMap<>();
                result.put("propertyChainAxiom", chain);
                result.put("label", label);
                result.put("_key", itemRepr);  // lxlviewer has some propertyChains of its own defined, this is used to match them
                return result;
            }

            String iri = getFullUri(itemId);

            if (iri == null) {
                return dummyChip(itemId);
            }

            Map<String, Object> m = new HashMap<>();
            iriPos.put(iri, m);
            return m;
        }

        void run() {
            Map<String, Map> cards = whelk.getCards(iriPos.keySet());
            for (Map.Entry<String, Map<String, Object>> entry : iriPos.entries()) {
                String key = entry.getKey();
                Map<String, Object> value = entry.getValue();
                Map<String, Object> card = cards.get(key);
                Map<String, Object> thing;
                if (card != null) {
                    thing = getEntry(card, key);
                    if (thing == null) {
                        thing = dummyChip(key);
                    }
                } else {
                    thing = dummyChip(key);
                }
                Map<String, Object> chip = (Map<String, Object>) ld.toChip(thing);
                value.putAll(chip);
            }
        }
    }

    private Map<String, Object> dummyChip(String itemId) {
        Map<String, Object> result = new HashMap<>();
        result.put(JsonLd.ID_KEY, itemId);
        result.put("label", itemId);
        return result;
    }

    /*
     * Read vocab term data from storage.
     *
     * Returns null if not found.
     *
     */
    private String getFullUri(String id) {
        try {
            if (vocabUri != null) {
                return vocabUri.resolve(id).toString();
            }
        } catch (IllegalArgumentException e) {
            // Couldn't resolve, which means id isn't a valid IRI.
            // No need to check the db.
            return null;
        }
        return null;
    }

    // FIXME move to Document or JsonLd
    private Map<String, Object> getEntry(Map<String, Object> jsonLd, String entryId) {
        // we rely on this convention for the time being.
        List<Map<String, Object>> graph = (List<Map<String, Object>>) jsonLd.get(JsonLd.GRAPH_KEY);
        if (graph != null) {
            for (Map<String, Object> item : graph) {
                if (entryId.equals(item.get(JsonLd.ID_KEY))) {
                    return item;
                }
            }
        }
        return null;
    }

    /**
     * Create a URL for '/find' with the specified query parameters.
     *
     */
    public String makeFindUrl(SearchType st, Map<String, Object> queryParameters, int offset) {
        Tuple2<List<String>, List<String>> initial = getInitialParamsAndKeys(st, queryParameters);
        List<String> params = initial.getV1();
        List<String> keys = initial.getV2();

        for (String k : keys) {
            Object v = queryParameters.get(k);
            if (v == null) {
                continue;
            }

            if (v instanceof List) {
                for (Object value : (List<?>) v) {
                    params.add(makeParam(k, value));
                }
            } else {
                params.add(makeParam(k, v));
            }
        }
        if (offset > 0) {
            params.add(makeParam("_offset", offset));
        }
        return "/find?" + String.join("&", params);
    }

    public String makeFindUrl(SearchType st, Map<String, Object> queryParameters) {
        return makeFindUrl(st, queryParameters, 0);
    }

    private String makeParam(Object key, Object value) {
        return escapeQueryParam(key) + "=" + escapeQueryParam(value);
    }

    private Tuple2<List<String>, List<String>> getInitialParamsAndKeys(SearchType st,
                                                                        Map<String, Object> queryParameters0) {
        Map<String, Object> queryParameters = new HashMap<>(queryParameters0);
        Tuple2<List<String>, List<String>> result;
        if (SearchType.ELASTIC.equals(st)) {
            result = getElasticParams(queryParameters);
        } else {
            result = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
        }
        return result;
    }

    private Tuple2<List<String>, List<String>> getElasticParams(Map<String, Object> queryParameters) {
        if (!queryParameters.containsKey("q") && !queryParameters.containsKey("o")) {
            queryParameters.put("q", "*");
        }

        String query = (String) queryParameters.remove("q");
        List<String> initialParams = new ArrayList<>();
        if (query != null) {
            initialParams.add(makeParam("q", query));
        }
        List<String> keys = new ArrayList<>(queryParameters.keySet());
        Collections.sort(keys);

        return new Tuple2<>(initialParams, keys);
    }

    public Map<String, Object> makePaginationLinks(SearchType st, Map<String, Object> pageParams,
                                                    int limit, int offset, int total) throws InvalidQueryException {
        if (limit == 0) {
            // we don't have anything to paginate over
            return new HashMap<>();
        }

        Map<String, Object> result = new HashMap<>();

        Offsets offsets = new Offsets(total, limit, offset);

        result.put("first", Map.of(JsonLd.ID_KEY, makeFindUrl(st, pageParams)));
        result.put("last", Map.of(JsonLd.ID_KEY, makeFindUrl(st, pageParams, offsets.last)));

        if (offsets.prev != null) {
            if (offsets.prev == 0) {
                result.put("previous", result.get("first"));
            } else {
                result.put("previous", Map.of(JsonLd.ID_KEY, makeFindUrl(st, pageParams, offsets.prev)));
            }
        }

        if (offsets.next != null) {
            result.put("next", Map.of(JsonLd.ID_KEY, makeFindUrl(st, pageParams, offsets.next)));
        }

        return result;
    }

    /**
     * Get limit and offset from query parameters.
     *
     * Use default values if not in query.
     *
     */
    public Tuple2<Integer, Integer> getLimitAndOffset(Map<String, Object> queryParams) throws InvalidQueryException {
        int limit = parseIntFromQueryParams("_limit", queryParams, DEFAULT_LIMIT);
        // don't let users get carried away.
        if (limit > MAX_LIMIT) {
            limit = DEFAULT_LIMIT;
        }

        if (limit < 0) {
            throw new InvalidQueryException("\"_limit\" query parameter can't be negative.");
        }

        int offset = parseIntFromQueryParams("_offset", queryParams, DEFAULT_OFFSET);

        if (offset < 0) {
            throw new InvalidQueryException("\"_offset\" query parameter can't be negative.");
        }

        return new Tuple2<>(limit, offset);
    }

    /*
     * Return specified query parameter parsed as int.
     *
     * Use default value if key not found.
     *
     */
    private int parseIntFromQueryParams(String key, Map<String, Object> queryParams, int defaultValue) {
        if (queryParams.containsKey(key)) {
            Object value = queryParams.get(key);

            // if someone supplies multiple values, we just pick the
            // first one and discard the rest.
            if (value instanceof List || value instanceof String[]) {
                if (value instanceof List && !((List<?>) value).isEmpty()) {
                    value = ((List<?>) value).getFirst();
                } else if (value instanceof String[] && ((String[]) value).length > 0) {
                    value = ((String[]) value)[0];
                } else {
                    return defaultValue;
                }
            }

            if (value instanceof String strValue) {
                try {
                    return Integer.parseInt(strValue);
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            } else if (value instanceof Number numValue) {
                return numValue.intValue();
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    /*
     * Get mappings and page params for specified query.
     *
     * Reserved parameters or parameters beginning with '_' are
     * filtered out.
     *
     */
    private Tuple2<List<Map<String, Object>>, Map<String, Object>> mapParams(Lookup lookup, Map<String, Object> params, Set<String> multiSelectable) {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> pageParams = new HashMap<>();
        List<String> reservedParams = getReservedParameters();

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String param = entry.getKey();
            Object paramValue = entry.getValue();

            if (param.startsWith("_") || reservedParams.contains(param)) {
                continue;
            }

            List<?> paramValues;
            if (paramValue instanceof List) {
                paramValues = (List<?>) paramValue;
            } else if (paramValue instanceof Object[]) {
                paramValues = Arrays.asList((Object[]) paramValue);
            } else {
                paramValues = List.of(paramValue);
            }

            for (Object val : paramValues) {
                String valueProp;
                String termKey;
                Object value;

                if (param.equals(JsonLd.TYPE_KEY) || param.equals(JsonLd.ID_KEY)) {
                    valueProp = "object";
                    termKey = param;
                    Map<String, Object> chip = lookup.chip((String) val);
                    chip.put(JsonLd.ID_KEY, val);
                    value = chip;
                } else if (param.endsWith("." + JsonLd.ID_KEY)) {
                    valueProp = "object";
                    termKey = param.substring(0, param.length() - ("." + JsonLd.ID_KEY).length());
                    Map<String, Object> chip = lookup.chip((String) val);
                    chip.put(JsonLd.ID_KEY, val);
                    value = chip;
                } else {
                    valueProp = "value";
                    termKey = param;
                    value = val;
                }

                termKey = stripPrefix(termKey, ESQuery.AND_PREFIX);
                termKey = stripPrefix(termKey, ESQuery.OR_PREFIX);

                Map<String, Object> mapping = new HashMap<>();
                mapping.put("variable", param);
                mapping.put("predicate", lookup.chip(termKey));
                mapping.put(valueProp, value);
                if (multiSelectable.contains(param)) {
                    mapping.put("_selected", true);
                }
                result.add(mapping);

                if (!pageParams.containsKey(param)) {
                    pageParams.put(param, new ArrayList<>());
                }
                ((List<Object>) pageParams.get(param)).add(val);
            }
        }

        return new Tuple2<>(result, pageParams);
    }

    /*
     * Return a list of reserved query params
     */
    private List<String> getReservedParameters() {
        return Arrays.asList("q", "p", "o", "value", "_limit", "_offset", "_suggest", "_spell", JsonLd.Platform.COMPUTED_LABEL);
    }

    /*
     * Get value for reserved parameter from query.
     *
     * Query is a Map<String, String[]>, but for reserved parameters,
     * we only allow a single value, so we return the first element of
     * the String[] if found, null otherwise.
     *
     */
    private String getReservedQueryParameter(String name, Map<String, Object> queryParameters) {
        if (queryParameters.containsKey(name)) {
            Object value = queryParameters.get(name);
            // For reserved parameters, we assume only one value
            if (value instanceof List && !((List<?>) value).isEmpty()) {
                return (String) ((List<?>) value).getFirst();
            } else if (value instanceof String[] arr) {
                if (arr.length > 0) {
                    return arr[0];
                }
            } else if (value instanceof String) {
                return (String) value;
            }
            return null;
        } else {
            return null;
        }
    }

    private Object escapeQueryParam(Object input) {
        if (input instanceof String) {
            String escaped = QUERY_ESCAPER.escape((String) input);
            // We want pretty URIs, restore some characters which are inside query strings
            // https://tools.ietf.org/html/rfc3986#section-3.4
            return escaped
                    .replace("%3A", ":")
                    .replace("%2F", "/")
                    .replace("%40", "@");
        }
        return input;
    }

    public Map<String, Object> buildStatsReprFromSliceSpec(List<Map<String, Object>> sliceList) {
        Map<String, Object> statsfind = new HashMap<>();
        for (Map<String, Object> slice : sliceList) {
            List<?> dimensionChain = (List<?>) slice.get("dimensionChain");
            List<String> path = new ArrayList<>();
            for (Object item : dimensionChain) {
                if (item instanceof Map) {
                    String inverseOf = (String) ((Map<?, ?>) item).get("inverseOfTerm");
                    path.add("@reverse." + inverseOf);
                } else if ("rdf:type".equals(item)) {
                    path.add(JsonLd.TYPE_KEY);
                } else {
                    path.add((String) item);
                }
            }

            String leaf = path.getLast();
            Object leafVocab = ld.vocabIndex.get(leaf);
            if (!ld.isVocabTerm(leaf) && leafVocab != null && ld.isInstanceOf((Map<String, Object>) leafVocab, "ObjectProperty")) {
                path.add(JsonLd.ID_KEY);
            }
            String key = String.join(".", path);
            int limit = (Integer) slice.get("itemLimit");
            String connectiveStr = (String) slice.get("connective");
            ESQuery.Connective connective = OR.toString().equals(connectiveStr) ? OR : AND;

            Map<String, Object> statsValue = new HashMap<>();
            statsValue.put("sort", "value");
            statsValue.put("sortOrder", "desc");
            statsValue.put("size", limit);
            statsValue.put("connective", connective);

            if (slice.get("_matchMissing") != null) { // FIXME: what should it be called?
                statsValue.put("_matchMissing", slice.get("_matchMissing"));
            }

            statsfind.put(key, statsValue);
        }
        return statsfind;
    }
}

class Offsets {
    Integer prev;
    Integer next;
    Integer last;

    Offsets(Integer total, Integer limit, Integer offset) throws InvalidQueryException {
        if (limit <= 0) {
            throw new InvalidQueryException("\"limit\" must be greater than 0.");
        }

        if (offset < 0) {
            throw new InvalidQueryException("\"offset\" can't be negative.");
        }

        this.prev = offset - limit;
        if (this.prev < 0) {
            this.prev = null;
        }

        this.next = offset + limit;
        if (this.next >= total) {
            this.next = null;
        } else if (offset == 0) {
            this.next = limit;
        }

        if ((offset + limit) >= total) {
            this.last = offset;
        } else {
            if (total % limit == 0) {
                this.last = total - limit;
            } else {
                this.last = total - (total % limit);
            }
        }
    }
}

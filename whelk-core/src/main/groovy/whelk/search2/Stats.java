package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.PropertyValue;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.VocabTerm;
import whelk.util.DocumentUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.search2.querytree.PropertyValue.equalsLink;
import static whelk.search2.querytree.PropertyValue.equalsLiteral;
import static whelk.search2.querytree.PropertyValue.equalsVocabTerm;
import static whelk.search2.QueryUtil.getAlias;
import static whelk.search2.QueryUtil.makeFindUrl;
import static whelk.search2.QueryUtil.makeParams;

public class Stats {
    private final Disambiguate disambiguate;
    private final QueryResult queryResult;
    private final QueryParams queryParams;
    private final AppParams appParams;
    private final QueryTree queryTree;
    private final QueryUtil queryUtil;

    public Stats(Disambiguate disambiguate,
                 QueryUtil queryUtil,
                 QueryTree queryTree,
                 QueryResult queryResult,
                 QueryParams queryParams,
                 AppParams appParams) {
        this.disambiguate = disambiguate;
        this.queryResult = queryResult;
        this.queryParams = queryParams;
        this.appParams = appParams;
        this.queryTree = queryTree;
        this.queryUtil = queryUtil;
    }

    public Map<String, Object> build() {
        var sliceByDimension = getSliceByDimension();
        var boolFilters = getBoolFilters();
        // TODO: Use Multi search API instead of doing two elastic roundtrips?
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html
        var predicates = getCuratedPredicateLinks();
        return Map.of(JsonLd.ID_KEY, "#stats",
                "sliceByDimension", sliceByDimension,
                "_boolFilters", boolFilters,
                "_predicates", predicates);
    }

    private Map<String, Object> getSliceByDimension() {
        var buckets = collectBuckets();
        var rangeProps = appParams.statsRepr.getRangeProperties();
        return buildSliceByDimension(buckets, rangeProps);
    }

    // Problem: Same value in different fields will be counted twice, e.g. contribution.agent + instanceOf.contribution.agent
    private Map<String, Map<PropertyValue, Integer>> collectBuckets() {
        Map<String, AppParams.Slice> sliceByProperty = appParams.statsRepr.getSliceByProperty();

        Map<String, Map<PropertyValue, Integer>> propertyToBuckets = new LinkedHashMap<>();
        for (var agg : queryResult.aggs) {
            String property = agg.property();
            boolean isObjectProperty = disambiguate.isObjectProperty(property);
            boolean hasVocabValue = disambiguate.hasVocabValue(property);

            for (var b : agg.buckets()) {
                PropertyValue pv = hasVocabValue
                        ? equalsVocabTerm(property, b.value())
                        : (isObjectProperty ? equalsLink(property, b.value()) : equalsLiteral(property, b.value()));
                var buckets = propertyToBuckets.computeIfAbsent(property, x -> new HashMap<>());
                buckets.compute(pv, (k, v) -> v == null ? b.count() : v + b.count());
            }
        }

        for (String property : sliceByProperty.keySet()) {
            var buckets = propertyToBuckets.remove(property);
            if (buckets != null) {
                int maxBuckets = sliceByProperty.get(property).size();
                Map<PropertyValue, Integer> newBuckets = new LinkedHashMap<>();
                buckets.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(Math.min(maxBuckets, buckets.size()))
                        .forEach(entry -> newBuckets.put(entry.getKey(), entry.getValue()));
                propertyToBuckets.put(property, newBuckets);
            }
        }

        return propertyToBuckets;
    }

    private Map<String, Object> buildSliceByDimension(Map<String, Map<PropertyValue, Integer>> propToBuckets, Set<String> rangeProps) {
        Map<String, String> nonQueryParams = queryParams.getNonQueryParams(0);

        Map<String, Object> sliceByDimension = new LinkedHashMap<>();

        propToBuckets.forEach((property, buckets) -> {
            var sliceNode = new LinkedHashMap<>();
            var isRange = rangeProps.contains(property);
            var observations = getObservations(buckets, isRange ? queryTree.removeTopLevelPvRangeNodes(property) : queryTree, nonQueryParams);
            if (!observations.isEmpty()) {
                if (isRange) {
                    sliceNode.put("search", getRangeTemplate(property, makeParams(nonQueryParams)));
                }
                sliceNode.put("dimension", property);
                sliceNode.put("observation", observations);
                getAlias(property, queryTree.getOutsetType()).ifPresent(a -> sliceNode.put("alias", a));
                sliceByDimension.put(property, sliceNode);
            }
        });

        return sliceByDimension;
    }

    private List<Map<String, Object>> getObservations(Map<PropertyValue, Integer> buckets, QueryTree qt, Map<String, String> nonQueryParams) {
        List<Map<String, Object>> observations = new ArrayList<>();

        buckets.forEach((pv, count) -> {
            Map<String, Object> observation = new LinkedHashMap<>();
            boolean queried = qt.getTopLevelPvNodes().contains(pv) || pv.value().string().equals(nonQueryParams.get("_o"));
            if (!queried) {
                observation.put("totalItems", count);
                var url = makeFindUrl(qt.andExtend(pv), nonQueryParams);
                observation.put("view", Map.of(JsonLd.ID_KEY, url));
                observation.put("object", queryUtil.lookUp(pv.value()));
                observations.add(observation);
            }
        });

        return observations;
    }

    private Map<String, Object> getRangeTemplate(String property, List<String> nonQueryParams) {
        var GtLtNodes = queryTree.getTopLevelPvNodes().stream()
                .filter(pv -> pv.property().equals(property))
                .filter(pv -> switch (pv.operator()) {
                    case EQUALS, NOT_EQUALS -> false;
                    case GREATER_THAN_OR_EQUALS, GREATER_THAN, LESS_THAN_OR_EQUALS, LESS_THAN -> true;
                })
                .filter(pv -> pv.value().isNumeric())
                .toList();

        String min = null;
        String max = null;

        for (var pv : GtLtNodes) {
            var orEquals = pv.toOrEquals();
            if (orEquals.operator() == Operator.GREATER_THAN_OR_EQUALS && min == null) {
                min = orEquals.value().string();
            } else if (orEquals.operator() == Operator.LESS_THAN_OR_EQUALS && max == null) {
                max = orEquals.value().string();
            } else {
                // Not a proper range, reset and abort
                min = null;
                max = null;
                break;
            }
        }

        var tree = queryTree.removeTopLevelPvNodesByOperator(property);

        Map<String, Object> template = new LinkedHashMap<>();

        var placeholderNode = new FreeText(Operator.EQUALS, String.format("{?%s}", property));
        var templateQueryString = tree.andExtend(placeholderNode).toQueryString();
        var templateUrl = makeFindUrl(tree.getFreeTextPart(), templateQueryString, nonQueryParams);
        template.put("template", templateUrl);

        var mapping = new LinkedHashMap<>();
        mapping.put("variable", property);
        mapping.put(Operator.GREATER_THAN_OR_EQUALS.termKey, Objects.toString(min, ""));
        mapping.put(Operator.LESS_THAN_OR_EQUALS.termKey, Objects.toString(max, ""));
        template.put("mapping", mapping);

        return template;
    }

    private List<Map<String, Object>> getBoolFilters() {
        var nonQueryParams = queryParams.getNonQueryParams(0);
        List<Map<String, Object>> results = new ArrayList<>();
        var existing = queryTree.getTopLevelNodes();

        for (var f : appParams.siteFilters.optionalFilters()) {
            QueryTree newTree;
            boolean isSelected;
            Node filter = f.getAlias().orElse(f.getExplicit());

            if (existing.contains(filter)) {
                newTree = queryTree.removeTopLevelNode(filter);
                isSelected = true;
            } else {
                newTree = queryTree.andExtend(filter);
                isSelected = false;
            }

            Map<String, Object> res = new LinkedHashMap<>();
            // TODO: fix form
            res.put("totalItems", 0);
            res.put("object", Map.of(JsonLd.TYPE_KEY, "Resource", "prefLabelByLang", f.getPrefLabelByLang()));
            res.put("view", Map.of(JsonLd.ID_KEY, makeFindUrl(newTree, nonQueryParams)));
            res.put("_selected", isSelected);

            results.add(res);
        }

        return results;
    }

    // TODO naming things "curated predicate links" ??
    private List<Map<?, ?>> getCuratedPredicateLinks() {
        var o = getObject(queryParams);
        if (o == null) {
            return Collections.emptyList();
        }
        var curatedPredicates = queryUtil.curatedPredicates(o, appParams.relationFilters);
        if (curatedPredicates.isEmpty()) {
            return Collections.emptyList();
        }
        QueryResult queryRes = new QueryResult(queryUtil.query(getCuratedPredicateEsQueryDsl(o, curatedPredicates)));
        return predicateLinks(queryRes.pAggs, o, queryParams.getNonQueryParams(0));
    }

    private Map<String, Object> getCuratedPredicateEsQueryDsl(Entity o, List<String> curatedPredicates) {
        var queryDsl = new LinkedHashMap<String, Object>();
        queryDsl.put("query", new PathValue("_links", o.id()).toEs());
        queryDsl.put("size", 0);
        queryDsl.put("from", 0);
        queryDsl.put("aggs", Aggs.buildPAggQuery(o, curatedPredicates, disambiguate));
        queryDsl.put("track_total_hits", true);
        return queryDsl;
    }

    private List<Map<?, ?>> predicateLinks(List<Aggs.Bucket> aggs, Entity object,
                                           Map<String, String> nonQueryParams) {
        var result = new ArrayList<Map<?, ?>>();
        Set<String> selected = new HashSet<>();
        if (nonQueryParams.containsKey(QueryParams.ApiParams.PREDICATES)) {
            selected.add(nonQueryParams.get(QueryParams.ApiParams.PREDICATES));
        }

        Map<String, Integer> counts = aggs.stream().collect(Collectors.toMap(Aggs.Bucket::value, Aggs.Bucket::count));

        for (String p : queryUtil.curatedPredicates(object, appParams.relationFilters)) {
            if (!counts.containsKey(p)) {
                continue;
            }

            int count = counts.get(p);

            if (count > 0) {
                Map<String, String> params = new HashMap<>(nonQueryParams);
                params.put(QueryParams.ApiParams.PREDICATES, p);
                result.add(Map.of(
                        "totalItems", count,
                        "view", Map.of(JsonLd.ID_KEY, makeFindUrl(makeParams(params))),
                        "object", queryUtil.lookUp(new VocabTerm(p)),
                        "_selected", selected.contains(p)
                ));
            }
        }

        return result;
    }

    private Entity getObject(QueryParams queryParams) {
        var object = queryParams.object;
        if (object != null) {
            return queryUtil.loadThing(object)
                    .map(thing -> thing.getOrDefault(JsonLd.TYPE_KEY, null))
                    .map(JsonLd::asList)
                    .map(types -> new Entity(object, (String) types.getFirst()))
                    .orElse(null);
        }
        return null;
    }
}

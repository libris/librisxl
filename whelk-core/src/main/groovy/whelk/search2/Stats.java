package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Literal;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.PropertyValue;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.Value;
import whelk.search2.querytree.VocabTerm;

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
import java.util.stream.Collectors;

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
    private Map<Property, Map<PropertyValue, Integer>> collectBuckets() {
        Map<Property, AppParams.Slice> sliceParamsByProperty = appParams.statsRepr.getSliceByProperty();
        Map<String, Map<String, Integer>> propertyNameToBucketCounts = new LinkedHashMap<>();

        // TODO: Decide how to handle properties that can appear at both instance and work level.
        //  Probably not the best idea to just add the counts together like we do now, since it's both inconvenient
        //  and not guaranteed to produce a correct number.
        for (var agg : queryResult.aggs) {
            for (var b : agg.buckets()) {
                var buckets = propertyNameToBucketCounts.computeIfAbsent(agg.property(), x -> new HashMap<>());
                buckets.compute(b.value(), (k, v) -> v == null ? b.count() : v + b.count());
            }
        }

        Map<Property, Map<PropertyValue, Integer>> propertyToBucketCounts = new LinkedHashMap<>();

        for (Property property : sliceParamsByProperty.keySet()) {
            var buckets = propertyNameToBucketCounts.get(property.name());
            if (buckets != null) {
                int maxBuckets = sliceParamsByProperty.get(property).size();
                Map<PropertyValue, Integer> newBuckets = new LinkedHashMap<>();
                buckets.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(Math.min(maxBuckets, buckets.size()))
                        .forEach(entry -> {
                            String bucketKey = entry.getKey();
                            int count = entry.getValue();
                            Value v;
                            if (property.hasVocabValue()) {
                                v = new VocabTerm(bucketKey, disambiguate.getDefinition(bucketKey));
                            } else if (property.isObjectProperty()) {
                                v = new Link(bucketKey, disambiguate.getChip(bucketKey));
                            } else {
                                v = new Literal(bucketKey);
                            }
                            newBuckets.put(new PropertyValue(property, Operator.EQUALS, v), count);
                        });
                propertyToBucketCounts.put(property, newBuckets);
            }
        }

        return propertyToBucketCounts;
    }

    private Map<String, Object> buildSliceByDimension(Map<Property, Map<PropertyValue, Integer>> propToBuckets, Set<Property> rangeProps) {
        Map<String, String> nonQueryParams = queryParams.getNonQueryParams(0);
        Map<Property, AppParams.Slice> sliceParamsByProperty = appParams.statsRepr.getSliceByProperty();

        Map<String, Object> sliceByDimension = new LinkedHashMap<>();

        propToBuckets.forEach((property, buckets) -> {
            var sliceNode = new LinkedHashMap<>();
            var isRange = rangeProps.contains(property);
            var observations = getObservations(buckets, isRange ? queryTree.removeTopLevelPropValueWithRangeIfPropEquals(property) : queryTree, nonQueryParams);
            if (!observations.isEmpty()) {
                if (isRange) {
                    sliceNode.put("search", getRangeTemplate(property, makeParams(nonQueryParams)));
                }
                sliceNode.put("dimension", property.name());
                sliceNode.put("observation", observations);
                sliceNode.put("maxItems", sliceParamsByProperty.get(property).size());
                sliceByDimension.put(property.name(), sliceNode);
            }
        });

        return sliceByDimension;
    }

    private List<Map<String, Object>> getObservations(Map<PropertyValue, Integer> buckets, QueryTree qt, Map<String, String> nonQueryParams) {
        List<Map<String, Object>> observations = new ArrayList<>();

        buckets.forEach((pv, count) -> {
            Map<String, Object> observation = new LinkedHashMap<>();
            boolean queried = qt.getTopLevelPvNodes().contains(pv)
                    || pv.value().string().equals(nonQueryParams.get(QueryParams.ApiParams.OBJECT));
            if (!queried) {
                observation.put("totalItems", count);
                var url = makeFindUrl(qt.addToTopLevel(pv), nonQueryParams);
                observation.put("view", Map.of(JsonLd.ID_KEY, url));
                observation.put("object", pv.value().description());
                observations.add(observation);
            }
        });

        return observations;
    }

    private Map<String, Object> getRangeTemplate(Property property, List<String> nonQueryParams) {
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

        var tree = queryTree.removeTopLevelPropValueIfPropEquals(property);

        Map<String, Object> template = new LinkedHashMap<>();

        var placeholderNode = new FreeText(Operator.EQUALS, String.format("{?%s}", property.name()));
        var templateQueryString = tree.addToTopLevel(placeholderNode).toString();
        var templateUrl = makeFindUrl(tree.getTopLevelFreeText(), templateQueryString, nonQueryParams);
        template.put("template", templateUrl);

        var mapping = new LinkedHashMap<>();
        mapping.put("variable", property.name());
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
                newTree = queryTree.addToTopLevel(filter);
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
    private List<Map<String, Object>> getCuratedPredicateLinks() {
        var o = getObject(queryParams);
        if (o == null) {
            return Collections.emptyList();
        }
        var curatedPredicates = curatedPredicates(o, appParams.relationFilters);
        if (curatedPredicates.isEmpty()) {
            return Collections.emptyList();
        }
        QueryResult queryRes = new QueryResult(queryUtil.query(getCuratedPredicateEsQueryDsl(o, curatedPredicates)));
        return predicateLinks(queryRes.pAggs, o, queryParams.getNonQueryParams(0));
    }

    private List<String> curatedPredicates(Entity object, Map<String, List<String>> relationFilters) {
        return object.superclassesIncludingSelf()
                .stream()
                .filter(relationFilters::containsKey)
                .findFirst().map(relationFilters::get)
                .orElse(Collections.emptyList());
    }

    private Map<String, Object> getCuratedPredicateEsQueryDsl(Entity o, List<String> curatedPredicates) {
        var queryDsl = new LinkedHashMap<String, Object>();
        queryDsl.put("query", new PathValue("_links", Operator.EQUALS, o.id()).toEs());
        queryDsl.put("size", 0);
        queryDsl.put("from", 0);
        queryDsl.put("aggs", Aggs.buildPAggQuery(o, curatedPredicates, disambiguate));
        queryDsl.put("track_total_hits", true);
        return queryDsl;
    }

    private List<Map<String, Object>> predicateLinks(List<Aggs.Bucket> aggs, Entity object,
                                           Map<String, String> nonQueryParams) {
        var result = new ArrayList<Map<String, Object>>();
        Set<String> selected = new HashSet<>();
        if (nonQueryParams.containsKey(QueryParams.ApiParams.PREDICATES)) {
            selected.add(nonQueryParams.get(QueryParams.ApiParams.PREDICATES));
        }

        Map<String, Integer> counts = aggs.stream().collect(Collectors.toMap(Aggs.Bucket::value, Aggs.Bucket::count));

        for (String p : curatedPredicates(object, appParams.relationFilters)) {
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
                        "object", disambiguate.getDefinition(p),
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
                    .map(List::getFirst)
                    .map(type -> new Entity(object, (String) type, disambiguate.getSuperclasses((String) type)))
                    .orElse(null);
        }
        return null;
    }
}

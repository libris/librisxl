package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Path;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Aggs {
    private static final String NESTED_AGG_NAME = "n";

    public static final Integer DEFAULT_BUCKET_SIZE = 10;

    public record Aggregation(String property, String path, List<Bucket> buckets) {
    }

    public record Bucket(String value, int count) {
    }

    public static Map<String, Object> buildAggQuery(List<AppParams.Slice> sliceList,
                                                    JsonLd jsonLd,
                                                    Collection<String> types,
                                                    Function<String, Optional<String>> getNestedPath)
    {
        if (sliceList.isEmpty()) {
            return Map.of(JsonLd.TYPE_KEY,
                    Map.of("terms",
                            Map.of("field", JsonLd.TYPE_KEY)));
        }

        Map<String, Object> query = new LinkedHashMap<>();

        for (AppParams.Slice slice : sliceList) {
            Property property = slice.getProperty(jsonLd);

            if (!property.restrictions().isEmpty()) {
                // TODO: E.g. author (combining contribution.role and contribution.agent)
                throw new RuntimeException("Can't handle combined fields in aggs query");
            }

            new Path(property).expand(jsonLd)
                    .getAltPaths(jsonLd, types)
                    .stream()
                    .map(Path::fullSearchPath)
                    .forEach(path -> {
                        // Core agg query
                        var aggs = Map.of("terms",
                                Map.of("field", path,
                                        "size", slice.size(),
                                        "order", Map.of(slice.bucketSortKey(), slice.sortOrder())));

                        // If field is nested, wrap agg query with nested
                        var nested = getNestedPath.apply(path);
                        if (nested.isPresent()) {
                            aggs = Map.of("nested", Map.of("path", nested.get()),
                                    "aggs", Map.of(NESTED_AGG_NAME, aggs));
                        }

                        // Wrap agg query with a filter
                        var filter = QueryUtil.mustWrap(Collections.emptyList());
                        aggs = Map.of("aggs", Map.of(property.name(), aggs),
                                "filter", filter);

                        query.put(path, aggs);
                    });
        }

        return query;
    }

    public static Map<String, Object> buildPAggQuery(Link object,
                                                     List<Property> curatedPredicates,
                                                     JsonLd jsonLd,
                                                     Collection<String> types,
                                                     Function<String, Optional<String>> getNestedPath
    ) {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = curatedPredicates
                .stream()
                .collect(Collectors.toMap(
                        Property::name,
                        p -> new PathValue(p, Operator.EQUALS, object)
                                .expand(jsonLd, types.isEmpty() ? p.domain() : types)
                                .toEs(getNestedPath, List.of()))
                );

        if (!filters.isEmpty()) {
            query.put(QueryParams.ApiParams.PREDICATES, Map.of("filters", Map.of("filters", filters)));
        }

        return query;
    }

    public static List<Bucket> collectPAggResult(Map<String, Object> aggs) {
        return ((Map<?, ?>) aggs.getOrDefault("buckets", Map.of()))
                .entrySet()
                .stream()
                .map(e -> new Bucket((String) e.getKey(), (int) ((Map<?, ?>) e.getValue()).get("doc_count")))
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

            if (! (aggs.get(property) instanceof Map) )
                continue;
            var agg = (Map<?, ?>) aggs.get(property);

            if (agg == null) {
                continue;
            }

            if (agg.containsKey(NESTED_AGG_NAME)) {
                agg = (Map<?, ?>) agg.get(NESTED_AGG_NAME);
            }

            var buckets = ((List<?>) agg.get("buckets")).stream()
                    .map(Map.class::cast)
                    .map(b -> new Bucket((String) b.get("key"), (Integer) b.get("doc_count")))
                    .toList();

            aggregations.add(new Aggregation(property, path, buckets));
        }

        return aggregations;
    }
}

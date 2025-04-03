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

import static whelk.search2.QueryUtil.mustWrap;

public class Aggs {
    private static final String NESTED_AGG_NAME = "n";

    public static final Integer DEFAULT_BUCKET_SIZE = 10;

    public record Aggregation(String property, String path, List<Bucket> buckets) {
    }

    public record Bucket(String value, int count) {
    }

    public static Map<String, Object> buildAggQuery(AppParams.StatsRepr statsRepr,
                                                    JsonLd jsonLd,
                                                    Collection<String> types,
                                                    Function<String, Optional<String>> getNestedPath)
    {
        if (statsRepr.isEmpty()) {
            return Map.of(JsonLd.TYPE_KEY,
                    Map.of("terms",
                            Map.of("field", JsonLd.TYPE_KEY)));
        }

        Map<String, Object> query = new LinkedHashMap<>();

        for (AppParams.Slice slice : statsRepr.sliceList()) {
            Property property = slice.property();

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
                        var filter = mustWrap(Collections.emptyList());
                        aggs = Map.of("aggs", Map.of(property.name(), aggs),
                                "filter", filter);

                        query.put(path, aggs);
                    });
        }

        return query;
    }

    public static Map<String, Object> buildPAggQuery(Entity entity,
                                                     List<String> curatedPredicates,
                                                     JsonLd jsonLd,
                                                     Function<String, Optional<String>> getNestedPath) {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = curatedPredicates
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        p -> new PathValue(new Property(p, jsonLd), Operator.EQUALS, new Link(entity.id()))
                                .expand(jsonLd)
                                .toEs(getNestedPath, List.of()))
                );

        if (!filters.isEmpty()) {
            query.put(QueryParams.ApiParams.PREDICATES, Map.of("filters", Map.of("filters", filters)));
        }

        return query;
    }

    public static List<Bucket> collectPAggResult(Map<String, Object> esResponse) {
        var agg = getAggregations(esResponse).get(QueryParams.ApiParams.PREDICATES);
        if (agg == null) {
            return Collections.emptyList();
        }
        var buckets = (Map<?, ?>) agg.get("buckets");
        if (buckets == null) {
            return Collections.emptyList();
        }
        return buckets.entrySet()
                .stream()
                .map(e -> new Bucket((String) e.getKey(), (int) ((Map<?, ?>) e.getValue()).get("doc_count")))
                .toList();
    }

    public static List<Aggregation> collectAggResult(Map<String, Object> esResponse) {
        var aggregations = new ArrayList<Aggregation>();

        for (var e : getAggregations(esResponse).entrySet()) {
            var path = e.getKey();
            if (path.equals(QueryParams.ApiParams.PREDICATES)) {
                continue;
            }
            var property = e.getValue()
                    .keySet()
                    .stream()
                    .filter(Predicate.not("doc_count"::equals))
                    .map(String.class::cast)
                    .findFirst()
                    .get();
            var agg = (Map<?, ?>) e.getValue().get(property);

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

    private static Map<String, Map<?, ?>> getAggregations(Map<String, Object> esResponse) {
        return ((Map<?, ?>) esResponse.getOrDefault("aggregations", Collections.emptyMap()))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (Map<?, ?>) e.getValue()));
    }
}

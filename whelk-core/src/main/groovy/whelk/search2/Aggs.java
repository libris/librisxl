package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Path;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Aggs {
    private static final String NESTED_AGG_NAME = "n";

    public static final Integer DEFAULT_BUCKET_SIZE = 10;

    public record Aggregation(String property, String path, List<Bucket> buckets) {
    }

    public record Bucket(String value, int count) {
    }

    public static Map<String, Object> buildPAggQuery(Link object,
                                                     List<Property> curatedPredicates,
                                                     JsonLd jsonLd,
                                                     Collection<String> types,
                                                     EsMappings esMappings
    ) {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = curatedPredicates
                .stream()
                .collect(Collectors.toMap(
                        Property::name,
                        p -> new PathValue(p, Operator.EQUALS, object)
                                .expand(jsonLd, types.isEmpty() ? p.domain() : types)
                                .toEs(esMappings, List.of()))
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

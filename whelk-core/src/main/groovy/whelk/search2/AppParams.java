package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.FilterAlias;
import whelk.search2.querytree.Property;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.castToStringObjectMap;

public class AppParams {
    public final StatsRepr statsRepr;
    public final List<FilterAlias> filterAliases;
    public final Filters filters;

    public AppParams(Map<String, Object> appConfig) {
        this.statsRepr = getStatsRepr(appConfig);
        this.filterAliases = getFilterAliases(appConfig);
        this.filters = getFilters(appConfig);
    }

    public record StatsRepr(List<Slice> sliceList) {
        Map<String, Slice> getSliceByPropertyKey() {
            var m = new LinkedHashMap<String, Slice>();
            for (Slice s : sliceList()) {
                m.put(s.propertyKey(), s);
            }
            return m;
        }
    }

    public Map<String, FilterAlias> getFilterByAlias() {
        return filterAliases.stream().collect(Collectors.toMap(FilterAlias::alias, Function.identity()));
    }

    public record Filters(List<String> defaultFilters, List<String> optionalFilters, List<RelationFilter> relationFilter) {
    }

    public record RelationFilter(String objectType, Collection<String> predicates) {
    }

    public static class Slice {
        public static final Integer DEFAULT_BUCKET_SIZE = 10;

        private final String propertyKey;
        private final Sort.Order sortOrder;
        private final Sort.BucketSortKey bucketSortKey;
        private final int size;
        private final boolean isRange;
        private final Query.Connective defaultConnective;

        private Property property;

        public Slice(String propertyKey, Map<?, ?> settings) {
            this.propertyKey = propertyKey;
            this.sortOrder = getSortOrder(settings);
            this.bucketSortKey = getBucketSortKey(settings);
            this.size = getSize(settings);
            this.isRange = getRangeFlag(settings);
            this.defaultConnective = getConnective(settings);
        }

        public String propertyKey() {
            return propertyKey;
        }

        public String sortOrder() {
            return sortOrder.name();
        }

        public String bucketSortKey() {
            return bucketSortKey.esKey;
        }

        public int size() {
            return size;
        }

        public boolean isRange() {
            return isRange;
        }

        public Query.Connective defaultConnective() {
            return defaultConnective;
        }

        public Property getProperty(JsonLd jsonLd) {
            if (property == null) {
                this.property = new Property(propertyKey, jsonLd);
            }
            return property;
        }

        private Sort.Order getSortOrder(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("sortOrder"))
                    .map(Sort.Order::valueOf)
                    .orElse(Sort.Order.desc);
        }

        private Sort.BucketSortKey getBucketSortKey(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("sort"))
                    .map(Sort.BucketSortKey::valueOf)
                    .orElse(Sort.BucketSortKey.count);
        }

        private int getSize(Map<?, ?> settings) {
            return Optional.ofNullable((Integer) settings.get("size")).orElse(DEFAULT_BUCKET_SIZE);
        }

        private boolean getRangeFlag(Map<?, ?> settings) {
            return Optional.ofNullable((Boolean) settings.get("range"))
                    .orElse(false);
        }

        private Query.Connective getConnective(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("connective"))
                    .map(Query.Connective::valueOf)
                    .orElse(Query.Connective.AND);
        }
    }

    private StatsRepr getStatsRepr(Map<String, Object> appConfig) {
        return Optional.ofNullable((Map<?, ?>) appConfig.get("_statsRepr"))
                .map(Map::entrySet)
                .map(entries -> entries.stream().map(this::getSlice).toList())
                .map(StatsRepr::new)
                .orElse(new StatsRepr(Collections.emptyList()));
    }

    private Slice getSlice(Map.Entry<?, ?> statsReprEntry) {
        var p = (String) statsReprEntry.getKey();
        var settings = (Map<?, ?>) statsReprEntry.getValue();
        return new Slice(p, settings);
    }

    private static List<FilterAlias> getFilterAliases(Map<String, Object> appConfig) {
        return getAsListOfMap(appConfig, "_filterAliases").stream()
                .map(m -> new FilterAlias((String) m.get("alias"),
                        (String) m.get("filter"),
                        castToStringObjectMap(m.get("prefLabelByLang"))))
                .toList();
    }

    private static Filters getFilters(Map<String, Object> appConfig) {
        return new Filters(getFilter(appConfig, "_defaultSiteFilters"),
                getFilter(appConfig, "_optionalSiteFilters"),
                getRelationFilters(appConfig)
        );
    }

    private static List<String> getFilter(Map<String, Object> appConfig, String key) {
        return getAsListOfMap(appConfig, key).stream()
                .map(m -> (String) m.get("filter"))
                .toList();
    }

    private static List<RelationFilter> getRelationFilters(Map<String, Object> appConfig) {
        return getAsListOfMap(appConfig, "_relationFilters").stream()
                .map(m -> new RelationFilter((String) m.get("objectType"),
                        getAsList(m, "predicates").stream().map(String.class::cast).toList()))
                .toList();
    }

    private static List<?> getAsList(Map<String, Object> appConfig, String key) {
        return (List<?>) appConfig.getOrDefault(key, Collections.emptyList());
    }

    private static List<Map<String, Object>> getAsListOfMap(Map<String, Object> appConfig, String key) {
        return getAsList(appConfig, key).stream().map(QueryUtil::castToStringObjectMap).toList();
    }
}

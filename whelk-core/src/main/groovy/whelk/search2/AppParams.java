package whelk.search2;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Property;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Query.SearchMode.BASIC_SEARCH;
import static whelk.search2.Query.SearchMode.OBJECT_SEARCH;
import static whelk.search2.Query.SearchMode.PREDICATE_OBJECT_SEARCH;
import static whelk.search2.QueryUtil.castToStringObjectMap;

public class AppParams {
    public final StatsRepr statsRepr;
    public final SiteFilters siteFilters;
    public final Map<String, List<String>> relationFilters;

    public AppParams(Map<String, Object> appConfig) {
        this.statsRepr = getStatsRepr(appConfig);
        this.siteFilters = getSiteFilters(appConfig);
        this.relationFilters = getRelationFilters(appConfig);
    }

    public record StatsRepr(List<Slice> sliceList) {
        Map<String, Slice> getSliceByPropertyKey() {
            var m = new LinkedHashMap<String, Slice>();
            for (Slice s : sliceList()) {
                m.put(s.propertyKey(), s);
            }
            return m;
        }

        public Set<String> getRangeProperties() {
            return sliceList().stream()
                    .filter(AppParams.Slice::isRange)
                    .map(AppParams.Slice::propertyKey)
                    .collect(Collectors.toSet());
        }
    }

    public record SiteFilters(List<DefaultSiteFilter> defaultFilters, List<OptionalSiteFilter> optionalFilters) {
        public Set<Filter.AliasedFilter> getAliasedFilters() {
            return getAllFilters().stream()
                    .map(SiteFilter::filter)
                    .filter(Filter.AliasedFilter.class::isInstance)
                    .map(Filter.AliasedFilter.class::cast)
                    .collect(Collectors.toSet());
        }

        public void parse(Disambiguate disambiguate) throws InvalidQueryException {
            for (SiteFilter sf : getAllFilters()) {
                sf.filter().parse(disambiguate);
            }
        }

        public List<SiteFilter> getAllFilters() {
            return Stream.concat(defaultFilters.stream(), optionalFilters.stream()).map(SiteFilter.class::cast).toList();
        }
    }

    public sealed interface SiteFilter permits DefaultSiteFilter, OptionalSiteFilter {
        Filter filter();
        Set<Query.SearchMode> appliesTo();
    }

    public record DefaultSiteFilter(Filter filter, Set<Query.SearchMode> appliesTo) implements SiteFilter {
        public DefaultSiteFilter(String rawFilter, String application, Map<String, Filter.AliasedFilter> filterByAlias) {
            this(getFilter(rawFilter, filterByAlias), switch (application) {
                case "basicSearch" -> Set.of(BASIC_SEARCH);
                case "objectSearch" -> Set.of(OBJECT_SEARCH, PREDICATE_OBJECT_SEARCH);
                case null, default -> Query.SearchMode.asSet();
            });
        }

        private static Filter getFilter(String raw, Map<String, Filter.AliasedFilter> filterByAlias) {
            return filterByAlias.containsKey(raw) ? filterByAlias.get(raw) : new Filter(raw);
        }
    }

    public record OptionalSiteFilter(Filter.AliasedFilter filter, Set<Query.SearchMode> appliesTo) implements SiteFilter {
        public OptionalSiteFilter(String rawFilter, Map<String, Filter.AliasedFilter> filterByAlias) {
            this(filterByAlias.get(rawFilter), Query.SearchMode.asSet());
        }
    }

    public static class Slice {
        private final String propertyKey;
        private final Sort.Order sortOrder;
        private final Sort.BucketSortKey bucketSortKey;
        private final int size;
        private final boolean isRange;

        private Property property;

        public Slice(String propertyKey, Map<?, ?> settings) {
            this.propertyKey = propertyKey;
            this.sortOrder = getSortOrder(settings);
            this.bucketSortKey = getBucketSortKey(settings);
            this.size = getSize(settings);
            this.isRange = getRangeFlag(settings);
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
            return Optional.ofNullable((Integer) settings.get("size")).orElse(Aggs.DEFAULT_BUCKET_SIZE);
        }

        private boolean getRangeFlag(Map<?, ?> settings) {
            return Optional.ofNullable((Boolean) settings.get("range"))
                    .orElse(false);
        }
    }

    private StatsRepr getStatsRepr(Map<String, Object> appConfig) {
        return Optional.ofNullable((Map<?, ?>) appConfig.get("_statsRepr"))
                .map(Map::entrySet)
                .map(entries -> entries.stream().map(this::getSlice).toList())
                .map(StatsRepr::new)
                .orElse(new StatsRepr(Collections.emptyList()));
    }

    private SiteFilters getSiteFilters(Map<String, Object> appConfig) {
        Map<String, Filter.AliasedFilter> filterByAlias = getFilterByAlias(appConfig);
        List<DefaultSiteFilter> defaultSiteFilters = getDefaultSiteFilters(appConfig, filterByAlias);
        List<OptionalSiteFilter> optionalSiteFilters = getOptionalSiteFilters(appConfig, filterByAlias);
        return new SiteFilters(defaultSiteFilters, optionalSiteFilters);
    }

    private Map<String, List<String>> getRelationFilters(Map<String, Object> appConfig) {
        Map<String, List<String>> filters = new HashMap<>();
        for (var e : castToStringObjectMap(appConfig.get("_relationFilters")).entrySet()) {
            String cls = e.getKey();
            List<String> relations = ((List<?>) e.getValue()).stream().map(String.class::cast).toList();
            filters.put(cls, relations);
        }
        return filters;
    }

    private Slice getSlice(Map.Entry<?, ?> statsReprEntry) {
        var p = (String) statsReprEntry.getKey();
        var settings = (Map<?, ?>) statsReprEntry.getValue();
        return new Slice(p, settings);
    }

    private Map<String, Filter.AliasedFilter> getFilterByAlias(Map<String, Object> appConfig) {
        return getAsListOfMap(appConfig, "_filterAliases").stream()
                .map(m -> new Filter.AliasedFilter(
                        (String) m.get("alias"),
                        (String) m.get("filter"),
                        castToStringObjectMap(m.get("prefLabelByLang")))
                )
                .collect(Collectors.toMap(Filter.AliasedFilter::alias, Function.identity()));
    }

    private List<DefaultSiteFilter> getDefaultSiteFilters(Map<String, Object> appConfig, Map<String, Filter.AliasedFilter> filterByAlias) {
        return getAsListOfMap(appConfig, "_defaultSiteFilters").stream()
                .map(m -> new DefaultSiteFilter((String) m.get("filter"), (String) m.get("application"), filterByAlias))
                .toList();
    }

    private List<OptionalSiteFilter> getOptionalSiteFilters(Map<String, Object> appConfig, Map<String, Filter.AliasedFilter> filterByAlias) {
        return getAsListOfMap(appConfig, "_optionalSiteFilters").stream()
                .map(m -> new OptionalSiteFilter((String) m.get("filter"), filterByAlias))
                .toList();
    }

    private List<?> getAsList(Map<String, Object> appConfig, String key) {
        return (List<?>) appConfig.getOrDefault(key, Collections.emptyList());
    }

    private List<Map<String, Object>> getAsListOfMap(Map<String, Object> appConfig, String key) {
        return getAsList(appConfig, key).stream().map(QueryUtil::castToStringObjectMap).toList();
    }
}

package whelk.search2;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Property;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Query.SearchMode.STANDARD_SEARCH;
import static whelk.search2.Query.SearchMode.OBJECT_SEARCH;
import static whelk.search2.Query.SearchMode.PREDICATE_OBJECT_SEARCH;
import static whelk.search2.Query.SearchMode.SUGGEST;
import static whelk.search2.QueryParams.ApiParams.ALIAS;
import static whelk.search2.QueryUtil.castToStringObjectMap;

public class AppParams {
    public final StatsRepr statsRepr;
    public final SiteFilters siteFilters;
    public final Map<String, List<String>> relationFilters;

    public AppParams(Map<String, Object> appConfig, QueryParams queryParams) {
        this.statsRepr = getStatsRepr(appConfig);
        this.siteFilters = getSiteFilters(appConfig, queryParams);
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
                case "standardSearch" -> Set.of(STANDARD_SEARCH, SUGGEST);
                case "objectSearch" -> Set.of(OBJECT_SEARCH);
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

    private SiteFilters getSiteFilters(Map<String, Object> appConfig, QueryParams queryParams) {
        Map<String, Filter.AliasedFilter> filterByAlias = getFilterByAlias(appConfig, queryParams.aliased);
        List<DefaultSiteFilter> defaultSiteFilters = getDefaultSiteFilters(appConfig, filterByAlias);
        if (!queryParams.r.isEmpty()) {
            defaultSiteFilters = new ArrayList<>(defaultSiteFilters);
            defaultSiteFilters.add(new DefaultSiteFilter(new Filter(queryParams.r), Query.SearchMode.asSet()));
        }
        List<OptionalSiteFilter> optionalSiteFilters = getOptionalSiteFilters(appConfig, filterByAlias);
        List<OptionalSiteFilter> queryDefinedSiteFilters = getQueryDefinedSiteFilters(queryParams, filterByAlias);
        return new SiteFilters(defaultSiteFilters, Stream.concat(optionalSiteFilters.stream(), queryDefinedSiteFilters.stream()).toList());
    }

    private List<OptionalSiteFilter> getQueryDefinedSiteFilters(QueryParams queryParams, Map<String, Filter.AliasedFilter> filterByAlias) {
        return filterByAlias.entrySet().stream()
                .filter(e -> e.getKey().startsWith(asqPrefix(ALIAS)))
                .filter(e -> queryParams.q.contains(asqPrefix(e.getKey())))
                .map(e ->  new OptionalSiteFilter(e.getValue(), Query.SearchMode.asSet()))
                .toList();
    }

    public String asqPrefix(String queryParameter) {
        return queryParameter.replaceAll("_","");
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

    private Map<String, Filter.AliasedFilter> getFilterByAlias(Map<String, Object> appConfig, Map<String, String[]> aliasedParams) {
        Stream<Filter.AliasedFilter> queryDefined = aliasedParams.entrySet().stream()
                .map(e -> new Filter.QueryDefinedFilter(asqPrefix(e.getKey()), e.getValue()[0], new HashMap<>()));

        Stream<Filter.AliasedFilter> predefined = getAsListOfMap(appConfig, "_filterAliases").stream()
                .map(m -> new Filter.AliasedFilter(
                        (String) m.get("alias"),
                        (String) m.get("filter"),
                        castToStringObjectMap(m.get("prefLabelByLang")))
                );

        return Stream.concat(queryDefined, predefined).collect(Collectors.toMap(Filter.AliasedFilter::alias, Function.identity()));
    }

    private List<DefaultSiteFilter> getDefaultSiteFilters(Map<String, Object> appConfig, Map<String, Filter.AliasedFilter> filterByAlias) {
        return getAsListOfMap(appConfig, "_defaultSiteFilters").stream()
                .map(m -> new DefaultSiteFilter((String) m.get("filter"), (String) m.get("applyTo"), filterByAlias))
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

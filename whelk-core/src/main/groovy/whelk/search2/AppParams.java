package whelk.search2;

import whelk.JsonLd;
import whelk.search2.querytree.FilterAlias;
import whelk.search2.querytree.Property;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.search2.QueryUtil.castToStringObjectMap;

public class AppParams {
    public static final String DEFAULT_SITE_FILTERS = "defaultSiteFilters";

    public final List<Slice> sliceList;
    public final List<FilterAlias> filterAliases;
    public final Filters filters;

    public AppParams(Map<String, Object> appConfig, JsonLd jsonLd) {
        this.sliceList = parseSliceList(appConfig, jsonLd);
        this.filterAliases = parseFilterAliases(appConfig);
        this.filters = parseFilters(appConfig);
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
        private final int itemLimit;
        private final boolean isRange;
        private final Query.Connective defaultConnective;
        private final Slice subSlice;
        private Slice parentSlice = null;
        private final List<String> showIf;
        private final boolean shouldCountTopLevelDocs;

        private final Property property;

        public Slice(Map<?, ?> settings, JsonLd jsonLd) {
            var chain = ((List<?>) settings.get("dimensionChain")).stream().map(String::valueOf).toList();
            this.sortOrder = parseSortOrder(settings);
            this.bucketSortKey = parseBucketSortKey(settings);
            this.itemLimit = parseItemLimit(settings);
            this.isRange = parseRangeFlag(settings);
            this.defaultConnective = parseConnective(settings);
            this.subSlice = parseSubSlice(settings, jsonLd);
            this.showIf = parseShowIf(settings);
            this.property = Property.getProperty(String.join(".", chain), jsonLd);
            this.propertyKey = property.name();
            this.shouldCountTopLevelDocs = parseShouldCountTopLevelDocs(settings);
        }

        public Slice(Map<?, ?> settings, Slice parent, JsonLd jsonLd) {
            this(settings, jsonLd);
            parentSlice = parent;
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
            return itemLimit;
        }

        public boolean isRange() {
            return isRange;
        }

        public Query.Connective defaultConnective() {
            return defaultConnective;
        }

        public Slice subSlice() {
            return subSlice;
        }

        public Slice parentSlice() {
            return parentSlice;
        }

        public Property getProperty() {
            return property;
        }

        public boolean shouldCountTopLevelDocs() {
            return shouldCountTopLevelDocs;
        }

        public List<String> getShowIf() {
            return showIf;
        }
        
        private Sort.Order parseSortOrder(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("sortOrder"))
                    .map(Sort.Order::valueOf)
                    .orElse(Sort.Order.desc);
        }

        private Sort.BucketSortKey parseBucketSortKey(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("sort"))
                    .map(Sort.BucketSortKey::valueOf)
                    .orElse(Sort.BucketSortKey.count);
        }

        private int parseItemLimit(Map<?, ?> settings) {
            return Optional.ofNullable((Integer) settings.get("itemLimit")).orElse(DEFAULT_BUCKET_SIZE);
        }

        private boolean parseRangeFlag(Map<?, ?> settings) {
            return Optional.ofNullable((Boolean) settings.get("range"))
                    .orElse(false);
        }

        private Query.Connective parseConnective(Map<?, ?> settings) {
            return Optional.ofNullable((String) settings.get("connective"))
                    .map(Query.Connective::valueOf)
                    .orElse(Query.Connective.AND);
        }

        private Slice parseSubSlice(Map<?, ?> settings, JsonLd jsonLd) {
            return Optional.ofNullable((Map<?,?>) settings.get("slice"))
                    .map(s -> new Slice(s, this, jsonLd))
                    .orElse(null);
        }

        private List<String> parseShowIf(Map<?, ?> settings) {
            return Optional.ofNullable((List<?>) settings.get("showIf"))
                    .map(l -> l.stream().map(String::valueOf).collect(Collectors.toList()))
                    .orElse(Collections.emptyList());
        }

        private boolean parseShouldCountTopLevelDocs(Map<?, ?> settings) {
            return Optional.ofNullable((Boolean) settings.get("countTopLevelDocs")).orElse(false);
        }
    }


    private List<Slice> parseSliceList(Map<String, Object> appConfig, JsonLd jsonLd) {
        if (appConfig.containsKey("statistics")) {
            var s = (Map<?, ?>) appConfig.get("statistics");
            if (s.containsKey("sliceList")) {
                var sliceList = (List<Map<?, ?>>) s.get("sliceList");
                return sliceList.stream().map(settings -> new Slice(settings, jsonLd)).collect(Collectors.toList());
            }
        }

        return Collections.emptyList();
    }

    private static List<FilterAlias> parseFilterAliases(Map<String, Object> appConfig) {
        return getAsListOfMap(appConfig, "filterAliases").stream()
                .map(m -> new FilterAlias((String) m.get("alias"),
                        (String) m.get("filter"),
                        castToStringObjectMap(m.get("prefLabelByLang"))))
                .toList();
    }

    private static Filters parseFilters(Map<String, Object> appConfig) {
        return new Filters(parseFilter(appConfig, DEFAULT_SITE_FILTERS),
                parseFilter(appConfig, "optionalSiteFilters"),
                parseRelationFilters(appConfig)
        );
    }

    private static List<String> parseFilter(Map<String, Object> appConfig, String key) {
        return getAsListOfMap(appConfig, key).stream()
                .map(m -> (String) m.get("filter"))
                .toList();
    }

    private static List<RelationFilter> parseRelationFilters(Map<String, Object> appConfig) {
        return getAsListOfMap(appConfig, "relationFilters").stream()
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

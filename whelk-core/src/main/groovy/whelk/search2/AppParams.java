package whelk.search2;

import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.ActiveBoolFilter;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTreeBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AppParams {
    private final Disambiguate disambiguate;
    private final Whelk whelk;

    public final StatsRepr statsRepr;
    public final SiteFilters siteFilters;
    public final Map<String, List<String>> relationFilters;

    public AppParams(Map<String, Object> appConfig, Disambiguate disambiguate, Whelk whelk) {
        this.disambiguate = disambiguate;
        this.whelk = whelk;
        this.statsRepr = getStatsRepr(appConfig);
        this.siteFilters = getSiteFilters(appConfig);
        this.relationFilters = getRelationFilters(appConfig);
    }

    private StatsRepr getStatsRepr(Map<String, Object> appConfig) {
        return Optional.ofNullable((Map<?, ?>) appConfig.get("_statsRepr"))
                .map(Map::entrySet)
                .map(entries -> entries.stream().map(this::getSlice).toList())
                .map(StatsRepr::new)
                .orElse(new StatsRepr(Collections.emptyList()));
    }

    private SiteFilters getSiteFilters(Map<String, Object> appConfig) {
        var aliasedFilters = getAliasedFilters(appConfig);
        var optionalSiteFilters = getOptionalSiteFilters(appConfig, aliasedFilters);
        return new SiteFilters(aliasedFilters,
                getDefaultSiteFilters(appConfig, aliasedFilters),
                getDefaultSiteTypeFilters(appConfig, aliasedFilters),
                optionalSiteFilters);
    }

    private Map<String, List<String>> getRelationFilters(Map<String, Object> appConfig) {
        return Optional.ofNullable((Map<?, ?>) appConfig.get("_relationFilters"))
                .map(m -> m.entrySet()
                        .stream()
                        .map(e -> Map.entry((String) e.getKey(),
                                ((List<?>) e.getValue()).stream().map(String.class::cast).toList()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )
                .orElse(Collections.emptyMap());
    }

    private Slice getSlice(Map.Entry<?, ?> statsReprEntry) {
        var p = (String) statsReprEntry.getKey();
        var settings = (Map<?, ?>) statsReprEntry.getValue();
        return new Slice(new Property(p, whelk.getJsonld()), settings);
    }

    private Map<String, Filter> getAliasedFilters(Map<String, Object> appConfig) {
        var m = new LinkedHashMap<String, Filter>();

        for (var fa : getAsListOfMap(appConfig, "_filterAliases")) {
            String alias = (String) fa.get("alias");
            String filter = (String) fa.get("filter");
            m.put(alias, new Filter(filter, alias));
        }

        return m;
    }

    private List<Filter> getDefaultSiteFilters(Map<String, Object> appConfig, Map<String, Filter> aliasedFilters) {
        return getAsList(appConfig, "_defaultSiteFilters").stream()
                .map(m -> (String) ((Map<?, ?>) m).get("filter"))
                .map(f -> aliasedFilters.getOrDefault(f, new Filter(f)))
                .toList();
    }

    private List<Filter> getDefaultSiteTypeFilters(Map<String, Object> appConfig, Map<String, Filter> aliasedFilters) {
        List<Filter> list = new ArrayList<>();
        for (Object m : getAsList(appConfig, "_defaultSiteTypeFilters")) {
            String f = (String) ((Map<?, ?>) m).get("filter");
            Filter filter = aliasedFilters.getOrDefault(f, new Filter(f));
            list.add(filter);
        }
        return list;
    }

    private List<Filter> getOptionalSiteFilters(Map<String, Object> appConfig, Map<String, Filter> aliasedFilters) {
        var l = new ArrayList<Filter>();

        for (var osf : getAsListOfMap(appConfig, "_optionalSiteFilters")) {
            String f = (String) osf.get("filter");
            Filter filter = aliasedFilters.getOrDefault(f, new Filter(f));
            filter.setPrefLabelByLang((Map<?, ?>) osf.get("prefLabelByLang"));
            l.add(filter);
        }

        return l;
    }

    private List<?> getAsList(Map<String, Object> appConfig, String key) {
        return Optional.ofNullable((List<?>) appConfig.get(key))
                .orElse(Collections.emptyList());
    }

    private List<Map> getAsListOfMap(Map<String, Object> appConfig, String key) {
        return getAsList(appConfig, key).stream().map(Map.class::cast).toList();
    }

    public record StatsRepr(List<Slice> sliceList) {
        Map<Property, Slice> getSliceByProperty() {
            var m = new LinkedHashMap<Property, Slice>();
            for (Slice s : sliceList()) {
                m.put(s.property(), s);
            }
            return m;
        }

        public Set<Property> getRangeProperties() {
            return sliceList().stream()
                    .filter(AppParams.Slice::isRange)
                    .map(AppParams.Slice::property)
                    .collect(Collectors.toSet());
        }

        boolean isEmpty() {
            return sliceList().isEmpty();
        }
    }

    public record SiteFilters(Map<String, Filter> aliasToFilter,
                              List<Filter> defaultFilters,
                              List<Filter> defaultTypeFilters,
                              List<Filter> optionalFilters) {
        public Set<String> getSelectableFilterAliases() {
            return optionalFilters()
                    .stream()
                    .map(Filter::getAlias)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(n -> ((ActiveBoolFilter) n).alias())
                    .collect(Collectors.toSet());
        }

        public List<Node> getAllDefaultFilterNodes() {
            return new ArrayList<>() {
                {
                    addAll(getDefaultFilterNodes());
                    addAll(getDefaultTypeFilterNodes());
                }
            };
        }

        public List<Node> getDefaultFilterNodes() {
            return defaultFilters().stream().map(f -> f.getAlias().orElse(f.getExplicit())).toList();
        }

        public List<Node> getDefaultTypeFilterNodes() {
            return defaultTypeFilters().stream().map(f -> f.getAlias().orElse(f.getExplicit())).toList();
        }
    }

    public class Filter {
        private final String explicit;
        private String alias;
        private Node explicitNode;
        private Node aliasNode;
        private Map<?, ?> prefLabelByLang;

        Filter(String explicit) {
            this.explicit = explicit;
        }

        Filter(String explicit, String alias) {
            this.explicit = explicit;
            this.alias = alias;
        }

        public Node getExplicit() {
            if (explicitNode == null) {
                try {
                    this.explicitNode = QueryTreeBuilder.buildTree(explicit, disambiguate, whelk, AppParams.this.siteFilters.aliasToFilter());
                } catch (InvalidQueryException e) {
                    throw new RuntimeException(e);
                }
            }
            return explicitNode;
        }

        public Optional<Node> getAlias() {
            if (alias == null) {
                return Optional.empty();
            }
            if (aliasNode == null) {
                try {
                    this.aliasNode = QueryTreeBuilder.buildTree(alias, disambiguate, whelk, AppParams.this.siteFilters.aliasToFilter());
                } catch (InvalidQueryException e) {
                    throw new RuntimeException(e);
                }
            }
            return Optional.ofNullable(aliasNode);
        }

        public void setPrefLabelByLang(Map<?, ?> prefLabelByLang) {
            this.prefLabelByLang = prefLabelByLang;
        }

        public Map<?, ?> getPrefLabelByLang() {
            return Optional.ofNullable(prefLabelByLang).orElse(Collections.emptyMap());
        }
    }

    public static class Slice {
        private final Property property;
        private final Sort.Order sortOrder;
        private final Sort.BucketSortKey bucketSortKey;
        private final int size;
        private final boolean isRange;

        Slice(Property property, Map<?, ?> settings) {
            this.property = property;
            this.sortOrder = getSortOrder(settings);
            this.bucketSortKey = getBucketSortKey(settings);
            this.size = getSize(settings);
            this.isRange = getRangeFlag(settings);
        }

        public Property property() {
            return property;
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
}

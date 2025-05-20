package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.*;
import whelk.util.DocumentUtil;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.component.ElasticSearch.flattenedLangMapKey;
import static whelk.search2.EsBoost.addBoosts;
import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.search2.QueryUtil.makeFindUrlNoOffset;

public class Query {
    protected final Whelk whelk;

    protected final QueryParams queryParams;
    protected final AppParams appParams;
    protected final QueryTree queryTree;
    protected final ESSettings esSettings;

    private final Disambiguate disambiguate;
    private final LinkLoader linkLoader;
    private final SelectedFilters selectedFilters;
    private final Stats stats;

    protected Object esQueryDsl;
    protected QueryResult queryResult;

    public enum SearchMode {
        STANDARD_SEARCH,
        OBJECT_SEARCH,
        PREDICATE_OBJECT_SEARCH;

        public static Set<SearchMode> asSet() {
            return Set.of(values());
        }
    }

    public enum Connective {
        AND,
        OR
    }

    public static final String NESTED_AGG_NAME = "n";

    public Query(QueryParams queryParams,
                 AppParams appParams,
                 VocabMappings vocabMappings,
                 ESSettings esSettings,
                 Whelk whelk) throws InvalidQueryException
    {
        this.queryParams = queryParams;
        this.appParams = appParams;
        this.disambiguate = new Disambiguate(vocabMappings, appParams, whelk.getJsonld());
        appParams.siteFilters.parse(disambiguate);
        this.esSettings = esSettings;
        this.queryTree = new QueryTree(queryParams.q, disambiguate);
        this.whelk = whelk;
        this.linkLoader = new LinkLoader();
        this.selectedFilters = queryParams.skipStats ? new SelectedFilters(queryTree, appParams.siteFilters) : new SelectedFilters(queryTree, appParams);
        this.stats = new Stats();
    }

    public static Query init(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        return switch (getSearchMode(queryParams)) {
            case STANDARD_SEARCH -> new Query(queryParams, appParams, vocabMappings, esSettings, whelk);
            case OBJECT_SEARCH -> new ObjectQuery(queryParams, appParams, vocabMappings, esSettings, whelk);
            case PREDICATE_OBJECT_SEARCH -> new PredicateObjectQuery(queryParams, appParams, vocabMappings, esSettings, whelk);
        };
    }

    public Map<String, Object> collectResults() {
        return getPartialCollectionView();
    }

    public String findUrl() {
        return QueryUtil.makeFindUrl(queryTree, queryParams);
    }

    protected Object doGetEsQueryDsl() {
        applySiteFilters(SearchMode.STANDARD_SEARCH);
        if (queryParams.skipStats) {
            return getEsQueryDsl(getEsQuery());
        } else {
            List<String> rulingTypes = queryTree.collectRulingTypes(whelk.getJsonld());
            return getEsQueryDsl(getEsQuery(), getEsAggQuery(rulingTypes), getPostFilter(rulingTypes));
        }
    }

    protected void applySiteFilters(SearchMode searchMode) {
        queryTree.applySiteFilters(searchMode, appParams.siteFilters, selectedFilters);
    }

    protected List<Map<String, Object>> predicateLinks() {
        return List.of();
    }

    protected QueryResult getQueryResult() {
        if (queryResult == null) {
            this.queryResult = new QueryResult(doQuery(getEsQueryDsl()), queryParams.debug);
        }
        return queryResult;
    }

    protected Map<String, Object> getEsQueryDsl(Map<String, Object> query) {
        return getEsQueryDsl(query, Map.of(), Map.of());
    }

    protected Map<String, Object> getEsQueryDsl(Map<String, Object> query, Map<String, Object> aggs, Map<String, Object> postFilter) {
        var queryDsl = new LinkedHashMap<String, Object>();

        queryDsl.put("query", query);
        queryDsl.put("size", queryParams.limit);
        queryDsl.put("from", queryParams.offset);
        queryDsl.put("sort", queryParams.sortBy.getSortClauses(this::getSortField));

        if (queryParams.spell.suggest && esSettings.mappings.isSpellCheckAvailable()) {
            var spellQuery = Spell.getSpellQuery(queryTree);
            if (spellQuery.isPresent()) {
                if (queryParams.spell.suggestOnly) {
                    return Map.of("suggest", spellQuery.get());
                } else {
                    queryDsl.put("suggest", spellQuery.get());
                }
            }
        }

        if (!aggs.isEmpty()) {
            queryDsl.put("aggs", aggs);
        }

        if (!postFilter.isEmpty()) {
            queryDsl.put("post_filter", postFilter);
        }

        queryDsl.put("track_total_hits", true);

        if (queryParams.debug.contains(QueryParams.Debug.ES_SCORE)) {
            queryDsl.put("explain", true);
            // Scores won't be calculated when also using sort unless explicitly asked for
            queryDsl.put("track_scores", true);
            queryDsl.put("fields", List.of("*"));
        }

        return queryDsl;
    }

    protected Map<String, Object> getEsQuery(QueryTree queryTree, Collection<String> rulingTypes) {
        List<Node> multiSelectedFilters = selectedFilters.getAllMultiSelected().values().stream().flatMap(List::stream).toList();
        var esQuery = queryTree.toEs(whelk.getJsonld(), esSettings.mappings, queryParams.boostFields, rulingTypes, multiSelectedFilters);
        return addBoosts(esQuery, queryParams.esScoreFunctions);
    }

    protected Map<String, Object> getEsAggQuery(Collection<String> rulingTypes) {
        return buildAggQuery(appParams.statsRepr.sliceList(), whelk.getJsonld(), rulingTypes, esSettings.mappings, selectedFilters);
    }

    protected Map<String, Object> getPostFilter(Collection<String> rulingTypes) {
        return getEsMultiSelectedFilters(selectedFilters.getAllMultiSelected(), rulingTypes, whelk.getJsonld(), esSettings.mappings);
    }

    private Map<String, Object> getPartialCollectionView() {
        var view = new LinkedHashMap<String, Object>();

        view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
        view.put(JsonLd.ID_KEY, findUrl());

        view.put("itemOffset", queryParams.offset);
        view.put("itemsPerPage", queryParams.limit);
        view.put("totalItems", getQueryResult().numHits);

        // TODO: Include _o search representation in search mapping?
        view.put("search", Map.of("mapping", List.of(queryTree.toSearchMapping(queryParams))));

        view.putAll(Pagination.makeLinks(getQueryResult().numHits, esSettings.maxItems(), queryTree, queryParams));

        view.put("items", getQueryResult().collectItems(this::applyLens));

        if (!queryParams.skipStats) {
            view.put("stats", stats.build());
            linkLoader.queue(stats.getLinks());
        }

        if (!getQueryResult().spell.isEmpty()) {
            view.put("_spell", Spell.buildSpellSuggestions(getQueryResult(), queryTree, queryParams));
        }

        view.put("maxItems", esSettings.maxItems());

        if (queryParams.debug.contains(QueryParams.Debug.ES_QUERY)) {
            view.put(QueryParams.ApiParams.DEBUG, Map.of(QueryParams.Debug.ES_QUERY, getEsQueryDsl()));
        }

        linkLoader.queue(queryTree.collectLinks());
        linkLoader.loadChips();

        return view;
    }

    private Map<?, ?> doQuery(Object dsl) {
        return dsl instanceof List<?> l
                ? whelk.elastic.multiQuery(l)
                : whelk.elastic.query((Map<?, ?>) dsl);
    }

    private static SearchMode getSearchMode(QueryParams queryParams) throws InvalidQueryException {
        if (queryParams.object != null) {
            return queryParams.predicates.isEmpty() ? SearchMode.OBJECT_SEARCH : SearchMode.PREDICATE_OBJECT_SEARCH;
        }
        if (!queryParams.q.isEmpty()) {
            return SearchMode.STANDARD_SEARCH;
        }
        throw new InvalidQueryException("Missing required query parameter: _q");
    }

    private Object getEsQueryDsl() {
        if (esQueryDsl == null) {
            this.esQueryDsl = doGetEsQueryDsl();
        }
        return esQueryDsl;
    }

    private Map<String, Object> getEsQuery() {
        return getEsQuery(queryTree, List.of());
    }

    private static Map<String, Object> getEsMultiSelectedFilters(Map<String, List<Node>> multiSelected,
                                                          Collection<String> rulingTypes,
                                                          JsonLd jsonLd,
                                                          EsMappings esMappings)
    {
        if (multiSelected.isEmpty()) {
            return Map.of();
        }
        List<Node> orGrouped = multiSelected.values()
                .stream()
                .map(selected -> selected.size() > 1 ? new Or(selected) : selected.getFirst())
                .toList();
        return new QueryTree(orGrouped.size() == 1 ? orGrouped.getFirst() : new And(orGrouped))
                .toEs(jsonLd, esMappings, List.of(), rulingTypes, List.of());
    }

    private String getSortField(String termPath) {
        var path = expandLangMapKeys(termPath);
        if (esSettings.mappings.isKeywordField(path) && !esSettings.mappings.isFourDigitField(path)) {
            return String.format("%s.keyword", path);
        } else {
            return termPath;
        }
    }

    private String expandLangMapKeys(String field) {
        var parts = field.split("\\.");
        if (parts.length > 0) {
            assert whelk != null;
            var lastIx = parts.length - 1;
            if (whelk.getJsonld().langContainerAlias.containsKey(parts[lastIx])) {
                parts[lastIx] = flattenedLangMapKey(parts[lastIx]);
                return String.join(".", parts);
            }
        }
        return field;
    }

    private Map<String, Object> applyLens(Map<String, Object> framedThing) {
        Set<String> preserveLinks = Stream.ofNullable(queryParams.object).collect(Collectors.toSet());

        var res = switch (queryParams.lens) {
            case "chips" -> whelk.getJsonld().toChip(framedThing, preserveLinks);
            case "full" -> removeSystemInternalProperties(framedThing);
            default -> whelk.getJsonld().toCard(framedThing, false, false, false, preserveLinks, true);
        };

        return castToStringObjectMap(res);
    }

    private static Map<String, Object> removeSystemInternalProperties(Map<String, Object> framedThing) {
        DocumentUtil.traverse(framedThing, (value, path) -> {
            if (!path.isEmpty() && ((String) path.getLast()).startsWith("_")) {
                return new DocumentUtil.Remove();
            }
            return DocumentUtil.NOP;
        });
        return framedThing;
    }

    private static Map<String, Object> buildAggQuery(List<AppParams.Slice> sliceList,
                                               JsonLd jsonLd,
                                               Collection<String> rulingTypes,
                                               EsMappings esMappings,
                                               SelectedFilters selectedFilters) {
        if (sliceList.isEmpty()) {
            return Map.of(JsonLd.TYPE_KEY,
                    Map.of("terms",
                            Map.of("field", JsonLd.TYPE_KEY)));
        }

        Map<String, List<Node>> multiSelected = selectedFilters.getAllMultiSelected();

        Map<String, Object> query = new LinkedHashMap<>();

        for (AppParams.Slice slice : sliceList) {
            String pKey = slice.propertyKey();

            if (!selectedFilters.isSelectable(pKey)) {
                continue;
            }

            Property property = slice.getProperty(jsonLd);

            if (!property.restrictions().isEmpty()) {
                // TODO: E.g. author (combining contribution.role and contribution.agent)
                throw new RuntimeException("Can't handle combined fields in aggs query");
            }

            new Path(property).expand(jsonLd)
                    .getAltPaths(jsonLd, rulingTypes)
                    .forEach(path -> {
                        Map<String, Object> aggs = path.getEsNestedStem(esMappings)
                                .map(nestedStem -> buildNestedAggQuery(path, slice, nestedStem))
                                .orElse(buildCoreAqqQuery(path, slice));
                        Map<String, List<Node>> mSelected = selectedFilters.isMultiSelectable(pKey)
                                ? new HashMap<>(multiSelected) {{ remove(pKey); }}
                                : multiSelected;
                        Map<String, Object> filter = getEsMultiSelectedFilters(mSelected, rulingTypes, jsonLd, esMappings);
                        query.put(path.fullEsSearchPath(), filterWrap(aggs, property.name(), filter));
                    });
        }

        return query;
    }

    private static Map<String, Object> buildCoreAqqQuery(Path path, AppParams.Slice slice) {
        return Map.of("terms",
                Map.of("field", path.fullEsSearchPath(),
                        "size", slice.size(),
                        "order", Map.of(slice.bucketSortKey(), slice.sortOrder())));
    }

    private static Map<String, Object> buildNestedAggQuery(Path path, AppParams.Slice slice, String nestedStem) {
        return Map.of("nested", Map.of("path", nestedStem),
                "aggs", Map.of(NESTED_AGG_NAME, buildCoreAqqQuery(path, slice)));
    }

    private static Map<String, Object> filterWrap(Map<String, Object> aggs, String property, Map<String, Object> filter) {
        return Map.of("aggs", Map.of(property, aggs),
                "filter", filter.isEmpty() ? QueryUtil.mustWrap(List.of()) : filter);
    }

    private class LinkLoader {
        private final Map<String, Collection<Link>> links = new HashMap<>();

        private void loadChips() {
            var cards = whelk.getCards(links.keySet());

            links.forEach((id, links) -> {
                var cardGraph = cards.get(id);
                if (cardGraph != null) {
                    var chip = castToStringObjectMap(whelk.getJsonld().toChip(new Document(cardGraph).getThing()));
                    links.forEach(link -> link.setChip(chip));
                } else {
                    links.forEach(link -> link.setChip(dummyChip(id)));
                }
            });
        }

        private Map<String, Object> dummyChip(String id) {
            return Map.of(
                    JsonLd.ID_KEY, id,
                    JsonLd.Rdfs.LABEL, id
            );
        }

        private void queue(Link link) {
            links.computeIfAbsent(link.iri(), k -> new ArrayList<>()).add(link);
        }

        private void queue(Collection<Link> links) {
            links.forEach(this::queue);
        }
    }

    private class Stats {
        private final List<Link> links = new ArrayList<>();

        private List<Link> getLinks() {
            return links;
        }

        private Map<String, Object> build() {
            var sliceByDimension = getSliceByDimension();
            var boolFilters = getBoolFilters();
            var predicates = predicateLinks();
            return Map.of(JsonLd.ID_KEY, "#stats",
                    "sliceByDimension", sliceByDimension,
                    "_boolFilters", boolFilters,
                    "_predicates", predicates);
        }

        private Map<String, Object> getSliceByDimension() {
            return buildSliceByDimension(collectBuckets());
        }

        // Problem: Same value in different fields will be counted twice, e.g. contribution.agent + instanceOf.contribution.agent
        private Map<Property, Map<PathValue, Integer>> collectBuckets() {
            Map<String, Map<String, Integer>> propertyKeyToBucketCounts = new LinkedHashMap<>();

            // TODO: Decide how to handle properties that can appear at both instance and work level.
            //  Probably not the best idea to just add the counts together like we do now, since it's both inconvenient
            //  and not guaranteed to produce a correct number.
            for (var agg : getQueryResult().aggs) {
                for (var b : agg.buckets()) {
                    var buckets = propertyKeyToBucketCounts.computeIfAbsent(agg.property(), x -> new HashMap<>());
                    buckets.compute(b.value(), (k, v) -> v == null ? b.count() : v + b.count());
                }
            }

            Map<Property, Map<PathValue, Integer>> propertyToBucketCounts = new LinkedHashMap<>();

            for (AppParams.Slice slice : appParams.statsRepr.sliceList()) {
                String propertyKey = slice.propertyKey();
                Property property = slice.getProperty(whelk.getJsonld());

                var buckets = propertyKeyToBucketCounts.get(propertyKey);
                if (buckets != null) {
                    int maxBuckets = slice.size();
                    Map<PathValue, Integer> newBuckets = new LinkedHashMap<>();
                    buckets.entrySet()
                            .stream()
                            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                            .limit(Math.min(maxBuckets, buckets.size()))
                            .forEach(entry -> {
                                String bucketKey = entry.getKey();
                                int count = entry.getValue();
                                Value v = disambiguate.getValueForProperty(property, bucketKey);
                                newBuckets.put(new PathValue(property, Operator.EQUALS, v), count);
                            });
                    propertyToBucketCounts.put(property, newBuckets);
                }
            }

            return propertyToBucketCounts;
        }

        private Map<String, Object> buildSliceByDimension(Map<Property, Map<PathValue, Integer>> propToBuckets) {
            Map<String, AppParams.Slice> sliceByPropertyKey = appParams.statsRepr.getSliceByPropertyKey();

            Map<String, Object> sliceByDimension = new LinkedHashMap<>();

            propToBuckets.forEach((property, buckets) -> {
                var propertyKey = property.name();
                if (!selectedFilters.isSelectable(propertyKey)) {
                    return;
                }
                var sliceNode = new LinkedHashMap<>();
                var observations = getObservations(propertyKey, buckets);
                if (!observations.isEmpty()) {
                    if (selectedFilters.isRangeFilter(propertyKey)) {
                        sliceNode.put("search", getRangeTemplate(propertyKey));
                    }
                    sliceNode.put("dimension", propertyKey);
                    sliceNode.put("observation", observations);
                    sliceNode.put("maxItems", sliceByPropertyKey.get(propertyKey).size());
                    sliceByDimension.put(propertyKey, sliceNode);
                }
            });

            return sliceByDimension;
        }

        private List<Map<String, Object>> getObservations(String propertyKey, Map<PathValue, Integer> buckets) {
            List<Map<String, Object>> observations = new ArrayList<>();

            Connective connective = selectedFilters.getConnective(propertyKey);

            QueryTree qt = selectedFilters.isRangeFilter(propertyKey)
                    ? queryTree.omitNodes(selectedFilters.getRangeSelected(propertyKey))
                    : queryTree;

            buckets.forEach((pv, count) -> {
                if (pv.value() instanceof Link l && l.iri().equals(queryParams.object)) {
                    // TODO: This check won't be needed if/when we remove facets from resource page.
                    return;
                }

                boolean isSelected = selectedFilters.isSelected(pv, propertyKey);

                Consumer<QueryTree> addObservation = alteredTree -> {
                    Map<String, Object> observation = new LinkedHashMap<>();

                    observation.put("totalItems", count);
                    observation.put("view", Map.of(JsonLd.ID_KEY, makeFindUrlNoOffset(alteredTree, queryParams)));
                    observation.put("object", pv.value().description());
                    if (connective == Connective.OR) {
                        observation.put("_selected", isSelected);
                    }

                    observations.add(observation);

                    if (pv.value() instanceof Link l) {
                        links.add(l);
                    }
                };

                switch (connective) {
                    case AND -> {
                        if (!isSelected) {
                            addObservation.accept(qt.addTopLevelNode(pv));
                        }
                    }
                    case OR -> {
                        var selected = selectedFilters.getSelected(propertyKey);
                        if (isSelected) {
                            selected.stream()
                                    .filter(pv::equals)
                                    .findFirst()
                                    .map(qt::omitNode)
                                    .ifPresent(addObservation);
                        } else {
                            var newSelected = new ArrayList<>(selected) {{ add(pv); }};
                            var alteredTree = qt.omitNodes(selected).addTopLevelNode(new Or(newSelected));
                            addObservation.accept(alteredTree);
                        }
                    }
                }
            });

            return observations;
        }

        private Map<String, Object> getRangeTemplate(String propertyKey) {
            FreeText placeholderNode = new FreeText(String.format("{?%s}", propertyKey));
            String templateQueryString = queryTree.omitNodes(selectedFilters.getSelected(propertyKey))
                    .addTopLevelNode(placeholderNode)
                    .toQueryString();
            String templateUrl = makeFindUrlNoOffset(templateQueryString, queryParams);

            Function<Operator, String> getLimit = op -> selectedFilters.getRangeSelected(propertyKey)
                    .stream()
                    .map(PathValue.class::cast)
                    .map(PathValue::toOrEquals)
                    .filter(pv -> pv.operator().equals(op))
                    .findFirst()
                    .map(PathValue::value)
                    .map(Value::toString)
                    .orElse("");

            String minLimit = getLimit.apply(Operator.GREATER_THAN_OR_EQUALS);
            String maxLimit = getLimit.apply(Operator.LESS_THAN_OR_EQUALS);

            String gtoe = Operator.GREATER_THAN_OR_EQUALS.termKey;
            String ltoe = Operator.LESS_THAN_OR_EQUALS.termKey;

            Map<String, String> mapping = Map.of(
                    "variable", propertyKey,
                    gtoe, minLimit,
                    ltoe, maxLimit
            );

            return Map.of(
                    "template", templateUrl,
                    "mapping", mapping
            );
        }

        private List<Map<String, Object>> getBoolFilters() {
            List<Map<String, Object>> results = new ArrayList<>();

            for (var of : appParams.siteFilters.optionalFilters()) {
                Filter.AliasedFilter f = of.filter();
                boolean isSelected = selectedFilters.isActivated(f);

                QueryTree alteredTree;
                if (isSelected) {
                    alteredTree = queryTree.removeTopLevelNodes(selectedFilters.getActivatingNodes(f));
                } else {
                    alteredTree = (selectedFilters.isExplicitlyDeactivated(f)
                            ? queryTree.removeTopLevelNodes(selectedFilters.getDeactivatingNodes(f))
                            : queryTree).addTopLevelNode(f.getActive());
                }

                Map<String, Object> res = new LinkedHashMap<>();
                // TODO: fix form
                res.put("totalItems", 0);
                res.put("object", f.description());
                res.put("view", Map.of(JsonLd.ID_KEY, makeFindUrlNoOffset(alteredTree, queryParams)));
                res.put("_selected", isSelected);

                results.add(res);
            }

            return results;
        }
    }
}

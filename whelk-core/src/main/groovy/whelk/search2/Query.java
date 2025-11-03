package whelk.search2;

import com.google.common.base.Predicates;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.*;
import whelk.util.DocumentUtil;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.component.ElasticSearch.flattenedLangMapKey;
import static whelk.search2.EsMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.EsMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.EsMappings.KEYWORD;
import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.search2.QueryUtil.makeViewFindUrl;
import static whelk.search2.QueryUtil.mustWrap;

public class Query {
    protected final Whelk whelk;

    protected final QueryParams queryParams;
    protected final AppParams appParams;
    protected final QueryTree qTree;
    protected final QueryTree rTree;
    protected final QueryTree sTree; // TODO: Naming
    protected final ESSettings esSettings;
    protected final Disambiguate disambiguate;

    private final LinkLoader linkLoader;
    private final Stats stats;
    private SelectedFacets selectedFacets;

    protected Object esQueryDsl;
    protected QueryResult queryResult;

    protected QueryTree fullQueryTree;

    public enum SearchMode {
        SUGGEST,
        STANDARD_SEARCH,
        OBJECT_SEARCH,
        PREDICATE_OBJECT_SEARCH
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
                 Whelk whelk) throws InvalidQueryException {
        this.queryParams = queryParams;
        this.appParams = appParams;
        this.disambiguate = new Disambiguate(vocabMappings, appParams.filterAliases, queryParams.aliased, whelk.getJsonld());
        this.esSettings = esSettings;
        this.whelk = whelk;
        this.qTree = new QueryTree(queryParams.q, disambiguate);
        this.rTree = new QueryTree(queryParams.r, disambiguate);
        this.sTree = new QueryTree(String.join(" ", appParams.filters.defaultFilters()), disambiguate); // FIXME
        this.linkLoader = new LinkLoader();
        this.stats = new Stats();
    }

    public static Query init(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        return switch (getSearchMode(queryParams)) {
            case STANDARD_SEARCH -> new Query(queryParams, appParams, vocabMappings, esSettings, whelk);
            case OBJECT_SEARCH -> new ObjectQuery(queryParams, appParams, vocabMappings, esSettings, whelk);
            case PREDICATE_OBJECT_SEARCH -> new PredicateObjectQuery(queryParams, appParams, vocabMappings, esSettings, whelk);
            case SUGGEST -> new SuggestQuery(queryParams, appParams, vocabMappings, esSettings, whelk);
        };
    }

    public Map<String, Object> collectResults() {
        return getPartialCollectionView();
    }

    public String findUrl() {
        return QueryUtil.makeFindUrl(qTree.toQueryString(), queryParams);
    }

    protected Object doGetEsQueryDsl() {
        if (queryParams.skipStats) {
            return getEsQueryDsl(getEsQuery());
        } else {
            List<String> subjectTypes = getFullQueryTree().getRdfSubjectTypesList();
            return getEsQueryDsl(getEsQuery(), getEsAggQuery(subjectTypes), getPostFilter(subjectTypes));
        }
    }

    protected QueryTree getFullQueryTree() {
        if (fullQueryTree == null) {
            fullQueryTree = getFullQueryTree(qTree);
        }
        return fullQueryTree;
    }

    protected QueryTree getFullQueryTree(QueryTree baseTree) {
        return mergeTrees(baseTree.reduce(whelk.getJsonld()), List.of(rTree, sTree));
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

    protected Map<String, Object> getEsQueryDsl(Map<String, Object> query, Map<String, Object> aggs) {
        return getEsQueryDsl(query, aggs, Map.of());
    }

    protected Map<String, Object> getEsQueryDsl(Map<String, Object> query, Map<String, Object> aggs, Map<String, Object> postFilter) {
        var queryDsl = new LinkedHashMap<String, Object>();

        queryDsl.put("query", query);
        queryDsl.put("size", queryParams.limit);
        queryDsl.put("from", queryParams.offset);
        queryDsl.put("sort", queryParams.sortBy.getSortClauses(this::getSortField));

        if (queryParams.spell.suggest && esSettings.mappings().isSpellCheckAvailable()) {
            var spellQuery = Spell.getSpellQuery(qTree);
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

    protected Map<String, Object> getEsQuery(QueryTree queryTree) {
        List<Node> mmSelectedFacets = queryParams.skipStats
                ? List.of()
                : getSelectedFacets().getAllMultiOrMenuSelected().values().stream().flatMap(List::stream).toList();
        ESSettings currentEsSettings = queryParams.boost != null ? esSettings.withBoostSettings(queryParams.boost) : esSettings;
        var mainQuery = queryTree.toEs(whelk.getJsonld(), currentEsSettings, mmSelectedFacets);
        var functionScore = currentEsSettings.boost().functionScore().toEs();
        var constantScore = currentEsSettings.boost().constantScore().toEs();
        return mustWrap(Stream.of(mainQuery, functionScore, constantScore).filter(Predicate.not(Map::isEmpty)).toList());
    }

    protected Map<String, Object> getEsAggQuery(Collection<String> rdfSubjectTypes) {
        return buildAggQuery(appParams.sliceList, whelk.getJsonld(), rdfSubjectTypes, esSettings, getSelectedFacets());
    }

    protected Map<String, Object> getPostFilter(Collection<String> rdfSubjectTypes) {
        return getEsMmSelectedFacets(getSelectedFacets().getAllMultiOrMenuSelected(), rdfSubjectTypes, whelk.getJsonld(), esSettings);
    }

    protected Map<String, Object> getPartialCollectionView() {
        var view = new LinkedHashMap<String, Object>();

        view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
        view.put(JsonLd.ID_KEY, findUrl());

        view.put("itemOffset", queryParams.offset);
        view.put("itemsPerPage", queryParams.limit);
        view.put("totalItems", getQueryResult().numHits);

        // TODO: Include _o search representation in search mapping?
        view.put("search", Map.of("mapping", getSearchMapping()));

        view.putAll(Pagination.makeLinks(getQueryResult().numHits, esSettings.maxItems(), qTree, queryParams));

        view.put("items", getQueryResult().collectItems(this::applyLens));

        if (!queryParams.skipStats) {
            view.put("stats", stats.build());
            linkLoader.queue(stats.getLinks());
        }

        if (!getQueryResult().spell.isEmpty()) {
            view.put("_spell", Spell.buildSpellSuggestions(getQueryResult(), qTree, queryParams));
        }

        view.put("maxItems", esSettings.maxItems());

        if (queryParams.debug.contains(QueryParams.Debug.ES_QUERY)) {
            view.put(QueryParams.ApiParams.DEBUG, Map.of(QueryParams.Debug.ES_QUERY, getEsQueryDsl()));
        }

        linkLoader.loadChips();

        return view;
    }

    private List<Map<String, Object>> getSearchMapping() {
        List<Map<String, Object>> mappings = new ArrayList<>();

        BiConsumer<QueryTree, String> addMapping = (tree, urlParam) -> {
            if (!tree.isEmpty()) {
                var mapping = new LinkedHashMap<>(tree.toSearchMapping(queryParams, urlParam));
                mapping.put("variable", urlParam);
                linkLoader.queue(tree.collectLinks());
                mappings.add(mapping);
            }
        };

        addMapping.accept(qTree, QueryParams.ApiParams.QUERY);
        addMapping.accept(rTree, QueryParams.ApiParams.CUSTOM_SITE_FILTER);

        return mappings;
    }

    private Map<?, ?> doQuery(Object dsl) {
        return dsl instanceof List<?> l
                ? whelk.elastic.multiQuery(l)
                : whelk.elastic.query((Map<?, ?>) dsl);
    }

    private List<FilterAlias> collectOptionalFilters() {
        var filterByAlias = appParams.getFilterByAlias();
        Stream<FilterAlias> appDefined = appParams.filters.optionalFilters().stream()
                .filter(filterByAlias::containsKey)
                .map(filterByAlias::get);
        Stream<FilterAlias.QueryDefinedAlias> queryDefined = queryParams.aliased.stream();
        return Stream.concat(appDefined, queryDefined).peek(this::parse).toList();
    }

    private void parse(FilterAlias fa) {
        try {
            fa.parse(disambiguate);
        } catch (InvalidQueryException e) {
            throw new RuntimeException(e);
        }
    }

    private static SearchMode getSearchMode(QueryParams queryParams) throws InvalidQueryException {
        if (queryParams.suggest) {
            return SearchMode.SUGGEST;
        }
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
        return getEsQuery(getFullQueryTree());
    }

    private QueryTree mergeTrees(QueryTree baseTree, List<QueryTree> other) {
        // TODO: How to handle e.g. "X AND ((typ:Agent AND isPartOf:X) OR (typ:Verk AND year:1990))"?
        var baseTreeRdfSubjectType = baseTree.getRdfSubjectType();
        if (baseTreeRdfSubjectType.isNoType()) {
            for (QueryTree o : other) {
                var otherRdfSubjectType = o.getRdfSubjectType();
                if (!otherRdfSubjectType.isNoType()) {
                    baseTree = baseTree.add(otherRdfSubjectType.asNode());
                    break;
                }
            }
        }
        QueryTree merged = baseTree;
        for (QueryTree o : other) {
            merged = merged.merge(o, whelk.getJsonld());
        }
        return merged;
    }

    private SelectedFacets getSelectedFacets() {
        if (selectedFacets == null) {
            // TODO: Check selected in _r too?
            this.selectedFacets = new SelectedFacets(qTree, appParams.sliceList);
        }
        return selectedFacets;
    }

    private static Map<String, Object> getEsMmSelectedFacets(Map<String, List<Node>> mmSelected,
                                                                Collection<String> rdfSubjectTypes,
                                                                JsonLd jsonLd,
                                                                ESSettings esSettings) {
        if (mmSelected.isEmpty()) {
            return Map.of();
        }
        List<Node> orGrouped = mmSelected.values()
                .stream()
                .map(selected -> selected.size() > 1 ? new Or(selected) : selected.getFirst())
                .toList();
        return (orGrouped.size() == 1 ? orGrouped.getFirst() : new And(orGrouped))
                .expand(jsonLd, rdfSubjectTypes)
                .toEs(esSettings);
    }

    private String getSortField(String termPath) {
        var path = expandLangMapKeys(termPath);
        if (esSettings.mappings().hasFourDigitsShortField(path)) {
            return String.format("%s%s", path, FOUR_DIGITS_SHORT_SUFFIX);
        }
        else if (esSettings.mappings().hasKeywordSubfield(path)) {
            return String.format("%s.%s", path, KEYWORD);
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

    private record AggContext(JsonLd jsonLd,
                              Map<String, List<Node>> mmSelected,
                              Collection<String> rdfSubjectTypes,
                              ESSettings esSettings,
                              SelectedFacets selectedFacets) { }

    private static Map<String, Object> buildAggQuery(List<AppParams.Slice> sliceList,
                                                     JsonLd jsonLd,
                                                     Collection<String> rdfSubjectTypes,
                                                     ESSettings esSettings,
                                                     SelectedFacets selectedFacets) {
        if (sliceList.isEmpty()) {
            return Map.of(JsonLd.TYPE_KEY,
                    Map.of("terms",
                            Map.of("field", JsonLd.TYPE_KEY)));
        }

        Map<String, List<Node>> mmSelected = selectedFacets.getAllMultiOrMenuSelected();

        Map<String, Object> query = new LinkedHashMap<>();

        var ctx = new AggContext(jsonLd, mmSelected, rdfSubjectTypes, esSettings, selectedFacets);
        for (AppParams.Slice slice : sliceList) {
            addSliceToAggQuery(query, slice, ctx);
        }

        return query;
    }

    private static void addSliceToAggQuery(Map<String, Object> query,
                                           AppParams.Slice slice,
                                           AggContext ctx) {

        String pKey = slice.propertyKey();

        if (!ctx.selectedFacets.isSelectable(pKey)) {
            return;
        }

        Property property = slice.getProperty();

        if (!property.restrictions().isEmpty()) {
            // TODO: E.g. author (combining contribution.role and contribution.agent)
            throw new RuntimeException("Can't handle combined fields in aggs query");
        }

        var p = new Path(property).expand(ctx.jsonLd);
        var paths = property instanceof Property.Ix // TODO for now exclude altPaths for _categoryByCollection
                ? List.of(p)
                : p.getAltPaths(ctx.jsonLd, ctx.rdfSubjectTypes);

        paths.forEach(path -> {
            String jsonPath = path.jsonForm();
            String field = ctx.esSettings.mappings().hasFourDigitsKeywordField(jsonPath)
                    ? String.format("%s%s", jsonPath, FOUR_DIGITS_KEYWORD_SUFFIX)
                    : (ctx.esSettings.mappings().hasKeywordSubfield(jsonPath) ? String.format("%s.%s", jsonPath, KEYWORD) : jsonPath);
            Map<String, Object> aggs = path.getEsNestedStem(ctx.esSettings.mappings())
                    .map(nestedStem -> buildNestedAggQuery(field, slice, nestedStem, ctx))
                    .orElse(buildCoreAqqQuery(field, slice, ctx));
            Map<String, List<Node>> mSelected = ctx.selectedFacets.isMultiOrMenu(pKey)
                    ? with(new HashMap<>(ctx.mmSelected), m -> {
                            m.remove(pKey);
                            // FIXME
                            if (slice.parentSlice() != null) {
                                m.remove(slice.parentSlice().propertyKey());
                            }
                            if (slice.subSlice() != null) {
                                m.remove(slice.subSlice().propertyKey());
                            }
                        })
                    : ctx.mmSelected;
            Map<String, Object> filter = getEsMmSelectedFacets(mSelected, ctx.rdfSubjectTypes, ctx.jsonLd, ctx.esSettings);
            query.put(field, filterWrap(aggs, property.name(), filter));
        });
    }
    
    private static Map<String, Object> buildCoreAqqQuery(String field, AppParams.Slice slice, AggContext ctx) {
        var q = Map.of("terms",
                Map.of("field", field,

                        "size", slice.size(),
                        "order", Map.of(slice.bucketSortKey(), slice.sortOrder())));

        if (slice.subSlice() != null) {
            q = new LinkedHashMap<>(q);

            Map<String, Object> query = new LinkedHashMap<>();
            addSliceToAggQuery(query, slice.subSlice(), ctx);
            q.put("aggs", query);
        }

        return castToStringObjectMap(q);
    }

    private static Map<String, Object> buildNestedAggQuery(String field, AppParams.Slice slice, String nestedStem, AggContext ctx) {
        return Map.of("nested", Map.of("path", nestedStem),
                "aggs", Map.of(NESTED_AGG_NAME, buildCoreAqqQuery(field, slice, ctx)));
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
            var sliceByDimension = collectBuckets().getSliceByDimension(appParams.sliceList, getSelectedFacets());
            var boolFilters = getBoolFilters();
            var predicates = predicateLinks();
            return Map.of(JsonLd.ID_KEY, "#stats",
                    "sliceByDimension", sliceByDimension,
                    "_boolFilters", boolFilters,
                    "_predicates", predicates);
        }

        private class Observation {
            PathValue object;
            int count = 0;
            int largestCount = 0;
            SliceListResult subSlices;
            boolean uncertain = false;

            void add(int count, List<QueryResult.Aggregation> subAggregations) {
                if (count > largestCount) {
                    largestCount = count;
                }
                uncertain = largestCount != count;

                this.count += count;

                if (subAggregations != null && !subAggregations.isEmpty()) {
                    if (subSlices == null) {
                        subSlices = new SliceListResult();
                    }

                    subSlices.add(subAggregations);
                }
            }

            int count() {
                return count;
            }

            public void collectValues(Set<String> resultValues) {
                if (subSlices != null) {
                    subSlices.collectValues(resultValues);
                }
            }
        }

        private class SliceResult {
            Map<String, Observation> buckets;

            void add(QueryResult.Bucket bucket) {
                if (buckets == null) {
                    buckets = new HashMap<>();
                }

                var observation = buckets.computeIfAbsent(bucket.value(), x -> new Observation());
                observation.add(bucket.count(), bucket.subAggregations());
            }

            private Predicate<Map.Entry<String, Observation>> isNarrower(Value parentValue) {
                return (Map.Entry<String, Observation> entry) -> {
                    if (//entry.getValue().object.value() instanceof Link narrower
                            JsonLd.looksLikeIri(entry.getKey())
                                    && parentValue instanceof Link broader) {
                        var narrower = entry.getKey();
                        return whelk.getRelations().isImpliedBy(broader.iri(), narrower);
                    }
                    return true;
                };
            }

            public List<Map<String, Object>> getObservations(AppParams.Slice slice, Value parentValue, List<Node> selectedValue, SelectedFacets selectedFacets) {
                if (buckets == null) {
                    return Collections.emptyList();
                }

                var property = slice.getProperty();

                String propertyKey = property.name();
                List<Map<String, Object>> observations = new ArrayList<>();

                Connective connective = selectedFacets.getConnective(propertyKey);

                QueryTree qt = selectedFacets.isRangeFilter(propertyKey)
                        ? qTree.remove(selectedFacets.getRangeSelected(propertyKey))
                        : qTree;

                this.buckets.entrySet()
                        .stream()
                        // TODO only do this for nested aggs of the same property etc etc
                        .filter(parentValue != null ? isNarrower(parentValue) : Predicates.alwaysTrue())
                        .sorted(Map.Entry.comparingByValue(Comparator.comparing(Observation::count).reversed()))
                        .limit(slice.size())
                        .forEach(entry -> {
                            // FIXME
                            String bucketKey = entry.getKey();
                            var o = entry.getValue();
                            int count = entry.getValue().count();
                            Value v = disambiguate.mapValueForProperty(property, bucketKey).orElse(new FreeText(bucketKey));
                            var pv = property instanceof Property.Ix ix
                                    ? new PathValue(ix.term(), Operator.EQUALS, v) // TODO this is just for the isSelected comparison
                                    : new PathValue(property, Operator.EQUALS, v);

                            if (pv.value() instanceof Link l && l.iri().equals(queryParams.object)) {
                                // TODO: This check won't be needed if/when we remove facets from resource page.
                                return;
                            }

                            // TODO
                            boolean isSelected = selectedValue!=null && !selectedValue.isEmpty()
                                    ? selectedValue.stream().anyMatch(n -> n instanceof PathValue pv2 && pv2.value() instanceof Link l && v instanceof Link l2 && l.iri().equals(l2.iri()))
                                    : selectedFacets.isSelected(pv, property.queryForm());

                            Consumer<QueryTree> addObservation = alteredTree -> {
                                Map<String, Object> observation = new LinkedHashMap<>();

                                observation.put("totalItems", count);
                                observation.put("view", Map.of(JsonLd.ID_KEY, makeViewFindUrl(alteredTree.toQueryString(), queryParams)));
                                observation.put("object", v instanceof Resource r ? r.description() : v.toString());
                                if (isSelected) {
                                    observation.put("_selected", true);
                                }
                                if (o.subSlices != null && slice.subSlice() != null) {
                                    observation.put("sliceByDimension", o.subSlices.getSliceByDimension(List.of(slice.subSlice()), selectedFacets, v, selectedValue));
                                }

                                observations.add(observation);

                                if (pv.value() instanceof Link l) {
                                    links.add(l);
                                }
                            };

                            if (getSelectedFacets().isMenuSelectable(propertyKey)) {
                                List<Node> selected = selectedValue != null ? selectedValue : Collections.emptyList();
                                addObservation.accept(qt.remove(selected).add(pv));
                                return;
                            }

                            switch (connective) {
                                case AND -> addObservation.accept(qt.add(pv));
                                case OR -> {
                                    var selected = selectedFacets.getSelected(propertyKey);
                                    if (isSelected) {
                                        selected.stream()
                                                .filter(pv::equals)
                                                .findFirst()
                                                .map(qt::remove)
                                                .ifPresent(addObservation);
                                    } else {
                                        if (selected.isEmpty()) {
                                            addObservation.accept(qt.add(pv));
                                        } else {
                                            var newSelected = with(new ArrayList<>(selected), l -> l.add(pv));
                                            var alteredTree = qt.remove(selected).add(new Or(newSelected));
                                            addObservation.accept(alteredTree);
                                        }
                                    }
                                }
                            }
                        });

                return observations;
            }

            public void collectValues(Set<String> resultValues) {
                if (buckets != null) {
                    resultValues.addAll(buckets.keySet());
                    for (var b : buckets.values()) {
                        b.collectValues(resultValues);
                    }
                }
            }
        }

        class SliceListResult {
            Map<String, SliceResult> sliceResults;

            public SliceListResult () {

            }


            void add(QueryResult.Aggregation aggregation) {
                if (sliceResults == null) {
                    sliceResults = new HashMap<>();
                }

                var sliceResult = sliceResults.computeIfAbsent(aggregation.property(), x -> new SliceResult());
                for (var bucket : aggregation.buckets()) {
                    sliceResult.add(bucket);
                }
            }

            void add(List<QueryResult.Aggregation> aggregations) {
                for (var a : aggregations) {
                    this.add(a);
                }
            }

            public Map<String, Object> getSliceByDimension(List<AppParams.Slice> slices, SelectedFacets selectedFacets) {
                return getSliceByDimension(slices, selectedFacets, null, null);
            }

            private Map<String, Object> getSliceByDimension(List<AppParams.Slice> slices, SelectedFacets selectedFacets, Value parentValue, List<Node> selectedValue) {
                Map<String, Object> result = new LinkedHashMap<>();

                slices.forEach(slice -> {
                    var property = slice.getProperty();
                    var propertyKey = property.name();

                    if (!selectedFacets.isSelectable(propertyKey)) {
                        return;
                    }

                    var sliceResult = sliceResults.get(propertyKey);
                    if (sliceResult == null) {
                        // FIXME ????
                        return;
                    }

                    // TODO
                    List<Node> mySelectedValue = selectedValue;
                    if (selectedFacets.isMenuSelectable(propertyKey) && parentValue == null && selectedValue == null) {
                        var values = new HashSet<String>();
                        sliceResult.collectValues(values);

                        var term = property instanceof Property.Ix ix
                                ? ix.term()
                                : property;

                        // TODO
                        mySelectedValue = qTree.findTopNodesByCondition(node -> node instanceof PathValue pv
                            && pv.value() instanceof Link link && values.contains(link.iri()));

//pv.path().expand(whelk.getJsonld()).firstProperty().map(p -> p.equals(property)).orElse(false)
//&& pv.value() instanceof Link link && values.contains(link.iri())

                    }

                    var sliceNode = new LinkedHashMap<>();
                    var observations = sliceResult.getObservations(slice, parentValue, mySelectedValue, selectedFacets);
                    if (!observations.isEmpty()) {
                        if (selectedFacets.isRangeFilter(propertyKey)) {
                            sliceNode.put("search", getRangeTemplate(propertyKey));
                        }
                        sliceNode.put("dimension", propertyKey);
                        sliceNode.put("observation", observations);
                        sliceNode.put("maxItems", slice.size());
                        sliceNode.put("_connective", selectedFacets.getConnective(propertyKey).name());
                        result.put(propertyKey, sliceNode);
                    }

                });

                return result;
            }

            private void collectValues(Set<String> resultValues) {
                if (sliceResults != null) {
                    sliceResults.values().forEach(sliceResult -> sliceResult.collectValues(resultValues));
                }
            }

            private Set<String> collectValues() {
                var result = new HashSet<String>();
                collectValues(result);
                return result;
            }

        }

        // TODO: Decide how to handle properties that can appear at both instance and work level.
        //  Probably not the best idea to just add the counts together like we do now, since it's both inconvenient
        //  and not guaranteed to produce a correct number.
        private SliceListResult collectBuckets() {
            var r = new SliceListResult();
            r.add(getQueryResult().aggs);
            return r;
        }

        private Map<String, Object> getRangeTemplate(String propertyKey) {
            FreeText placeholderNode = new FreeText(String.format("{?%s}", propertyKey));
            String templateQueryString = qTree.remove(getSelectedFacets().getSelected(propertyKey))
                    .add(placeholderNode)
                    .toQueryString();
            String templateUrl = QueryUtil.makeViewFindUrl(templateQueryString, queryParams);

            Function<Operator, String> getLimit = op -> getSelectedFacets().getRangeSelected(propertyKey)
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
            JsonLd jsonLd = whelk.getJsonld();

            for (FilterAlias fa : collectOptionalFilters()) {
                boolean isSelected = false;
                // TODO: Check _r too?
                List<Node> implied = qTree.findTopNodesByCondition(n -> fa.implies(n, jsonLd));
                QueryTree alteredTree = switch (implied.size()) {
                    case 0 -> qTree.add(fa);
                    case 1 -> {
                        Node impliedNode = implied.getFirst();
                        if (fa.equals(impliedNode)) {
                            isSelected = true;
                            yield qTree.remove(impliedNode);
                        } else if (fa.getParsed().equals(impliedNode)) {
                            isSelected = true;
                            yield qTree.remove(impliedNode);
                        } else {
                            yield qTree.replace(impliedNode, fa);
                        }
                    }
                    default -> {
                        if (new And(implied).implies(fa.getParsed(), jsonLd)) {
                            isSelected = true;
                            yield qTree.remove(implied);
                        } else {
                            yield qTree.remove(implied).add(fa);
                        }
                    }
                };

                Map<String, Object> res = new LinkedHashMap<>();
                // TODO: fix form
                res.put("totalItems", 0);
                res.put("object", fa.description());
                res.put("view", Map.of(JsonLd.ID_KEY, makeViewFindUrl(alteredTree.toQueryString(), queryParams)));
                res.put("_selected", isSelected);

                results.add(res);
            }

            return results;
        }
    }

    private static <T> T with(T t, Consumer<T> f) {
        f.accept(t);
        return t;
    }
}

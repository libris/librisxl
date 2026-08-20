package whelk.search2;

import com.google.common.base.Predicates;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.esquery.ESQuery;
import whelk.search2.esquery.ESQueryDefinition;
import whelk.search2.esquery.ESSettings;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.FilterAlias;
import whelk.search2.querytree.value.FreeText;
import whelk.search2.querytree.value.Link;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.RdfSubjectType;
import whelk.search2.querytree.value.Resource;
import whelk.search2.querytree.value.Value;
import whelk.search2.querytree.value.YearRange;
import whelk.util.FresnelUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.search2.QueryUtil.makeViewFindUrl;
import static whelk.search2.querytree.QueryTreeReducer.implies;

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

    private QueryResult queryResult;

    private QueryTree.MergedTree fullQueryTree;

    static final String WORK_CATEGORY = "librissearch:workCategory";
    static final String NONE_CATEGORY = "librissearch:noneCategory";

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

    public Query(QueryParams queryParams,
                 AppParams appParams,
                 ResourceLookup resourceLookup,
                 ESSettings esSettings,
                 Whelk whelk) throws InvalidQueryException {
        this.queryParams = queryParams;
        this.appParams = appParams;
        this.disambiguate = new Disambiguate(resourceLookup, appParams.filterAliases, queryParams.aliased, whelk.getJsonld());
        this.esSettings = esSettings;
        this.whelk = whelk;
        this.qTree = new QueryTree(queryParams.q, disambiguate);
        this.rTree = new QueryTree(queryParams.r, disambiguate);
        this.sTree = new QueryTree(String.join(" ", appParams.filters.defaultFilters()), disambiguate); // FIXME
        this.linkLoader = new LinkLoader();
        this.stats = new Stats();
    }

    public static Query init(QueryParams queryParams, AppParams appParams, ResourceLookup resourceLookup, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        return switch (getSearchMode(queryParams)) {
            case STANDARD_SEARCH -> new Query(queryParams, appParams, resourceLookup, esSettings, whelk);
            case OBJECT_SEARCH -> new ObjectQuery(queryParams, appParams, resourceLookup, esSettings, whelk);
            case PREDICATE_OBJECT_SEARCH -> new PredicateObjectQuery(queryParams, appParams, resourceLookup, esSettings, whelk);
            case SUGGEST -> new SuggestQuery(queryParams, appParams, resourceLookup.vocabMappings(), esSettings, whelk);
        };
    }

    public Map<String, Object> collectResults() {
        return getPartialCollectionView();
    }

    public String findUrl() {
        return QueryUtil.makeFindUrl(qTree.toQueryString(), queryParams);
    }

    protected QueryResult getQueryResult() {
        if (queryResult == null) {
            this.queryResult = new QueryResult(getEsQuery().run(), queryParams.debug);
        }
        return queryResult;
    }

    protected ESQuery getEsQuery() {
        return new ESQuery(prepareEsQuery().dsl(), findIndexNames(), whelk.elastic);
    }

    protected QueryTree.MergedTree getFullQueryTree() {
        if (fullQueryTree == null) {
            fullQueryTree = getFullQueryTree(qTree);
        }
        return fullQueryTree;
    }

    protected QueryTree.MergedTree getFullQueryTree(QueryTree baseTree) {
        return mergeTrees(baseTree.reduce(whelk.getJsonld()), List.of(rTree, sTree));
    }

    protected List<Map<String, Object>> predicateLinks() {
        return List.of();
    }

    protected Map<String, Object> getPartialCollectionView() {
        var view = new LinkedHashMap<String, Object>();

        view.put(JsonLd.TYPE_KEY, "PartialCollectionView");
        view.put(JsonLd.ID_KEY, findUrl());

        // TODO: Include _o search representation in search mapping?
        view.put("search", Map.of("mapping", getSearchMapping()));

        if (queryParams.mappingOnly) {
            linkLoader.loadChips();
            return view;
        }

        var anyLike = qTree.allDescendants().anyMatch(n -> n instanceof Condition c
                && c.operator() == Operator.LIKE
                && c.value() instanceof Link);
        if (anyLike) {
            // FIXME this depends on chips being queued in getSearchMapping()
            linkLoader.loadChips();
            qTree.allDescendants().forEach(n -> {
                if (n instanceof Condition c
                        && c.operator() == Operator.LIKE
                        && c.value() instanceof Link link) {
                    var needle = whelk.getFresnelUtil().asString(link.description(), FresnelUtil.Lenses.SEARCH_NEEDLE);
                    link.setSearchNeedle(needle);
                }
            });
        }

        view.put("itemOffset", queryParams.offset);
        view.put("itemsPerPage", queryParams.limit);
        view.put("totalItems", getQueryResult().numHits);

        view.putAll(Pagination.makeLinks(getQueryResult().numHits, esSettings.maxItems(), qTree, queryParams));

        view.put("items", getQueryResult().collectItems(this::applyLens));

        if (queryParams.stats.on) {
            view.put("stats", stats.build());
            linkLoader.queue(stats.getLinks());
        }

        if (!getQueryResult().spell.isEmpty()) {
            view.put("_spell", Spell.buildSpellSuggestions(getQueryResult(), qTree, queryParams));
        }

        view.put("maxItems", esSettings.maxItems());

        if (queryParams.debug.contains(QueryParams.Debug.ES_QUERY)) {
            view.put(QueryParams.ApiParams.DEBUG, Map.of(QueryParams.Debug.ES_QUERY, prepareEsQuery().dsl()));
        }

        linkLoader.loadChips();

        return view;
    }

    protected SelectedFacets getSelectedFacets() {
        if (selectedFacets == null) {
            // TODO: Check selected in _r too?
            this.selectedFacets = new SelectedFacets(qTree, appParams.sliceList);
        }
        return selectedFacets;
    }

    private ESQueryDefinition prepareEsQuery() {
        JsonLd ld = whelk.getJsonld();
        QueryTree.MergedTree queryTree = getFullQueryTree();

        ESSettings currentEsSettings = queryParams.boost != null
                ? esSettings.withBoostSettings(queryParams.boost)
                : esSettings;

        ESQueryDefinition.AggsDefinition aggsDefinition = queryParams.stats.on
                ? new ESQueryDefinition.AggsDefinition(appParams.sliceList, getSelectedFacets(), queryTree.getRdfSubjectType().typeNames())
                : null;

        return new ESQueryDefinition(queryTree, currentEsSettings, queryParams, ld, aggsDefinition, null);
    }

    private List<String> findIndexNames() {
        /* TODO?
        // remove type condition that exactly matches subindex content
        if (indexNames.size() == 1 && !indexNames.getFirst().equals(whelk.elastic.getBaseIndex())) {
            var baseType = whelk.elastic.getBaseTypeForSubIndex(indexNames.getFirst());
            var removeFromTopLevel = new Type(base, whelk.getJsonld())
            ...
        }
         */
        return getFullQueryTree().getRdfSubjectType()
                .typeNames()
                .stream()
                .map(whelk.elastic::getIndexForType)
                .toList();
    }

    private List<Map<String, Object>> getSearchMapping() {
        List<Map<String, Object>> mappings = new ArrayList<>();

        BiConsumer<QueryTree, String> addMapping = (tree, urlParam) -> {
            if (!tree.isAny()) {
                var mapping = new LinkedHashMap<>(tree.toSearchMapping(queryParams, urlParam));
                mapping.put("variable", urlParam);
                linkLoader.queue(tree.collectLinks());
                mappings.add(mapping);
            }
        };

        addMapping.accept(qTree, QueryParams.ApiParams.QUERY);
        addMapping.accept(rTree, QueryParams.ApiParams.CUSTOM_SITE_FILTER);
        addMapping.accept(sTree, AppParams.DEFAULT_SITE_FILTERS);

        return mappings;
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

    private static SearchMode getSearchMode(QueryParams queryParams) {
        if (queryParams.suggest) {
            return SearchMode.SUGGEST;
        }
        if (queryParams.object != null) {
            return queryParams.predicates.isEmpty() ? SearchMode.OBJECT_SEARCH : SearchMode.PREDICATE_OBJECT_SEARCH;
        }
        return SearchMode.STANDARD_SEARCH;
    }

    private QueryTree.MergedTree mergeTrees(QueryTree baseTree, List<QueryTree> other) {
        baseTree = establishSubjectTypeContext(baseTree, other);
        QueryTree merged = baseTree;
        for (QueryTree o : other) {
            merged = merged.merge(o, whelk.getJsonld());
        }
        return (QueryTree.MergedTree) merged;
    }

    private QueryTree establishSubjectTypeContext(QueryTree baseTree, List<QueryTree> other) {
        if (baseTree.getRdfSubjectType().hasType()) {
            return baseTree;
        }

        return other.stream()
                .map(QueryTree::getRdfSubjectType)
                .filter(RdfSubjectType::hasType)
                .findFirst()
                .map(type -> baseTree.add(type.typeNode()))
                .orElse(baseTree);
    }

    private Map<String, Object> applyLens(Map<String, Object> framedThing) {
        Set<String> preserveLinks = Stream.ofNullable(queryParams.object).collect(Collectors.toSet());

        var res = "chips".equals(queryParams.lens)
            ? whelk.getJsonld().toChip(framedThing, preserveLinks)
            : removeSystemInternalProperties(framedThing);

        return castToStringObjectMap(res);
    }

    private static Map<String, Object> removeSystemInternalProperties(Map<String, Object> framedThing) {
        framedThing.remove("_id");
        return framedThing;
    }

    private class LinkLoader {
        private final Map<String, Collection<Link>> linkMap = new HashMap<>();

        private void loadChips() {
            var chips = whelk.getChipCache().getChips(linkMap.keySet());

            linkMap.forEach((id, links) -> {
                var chip = chips.get(id);
                links.forEach(link -> link.setChip(chip));
            });

            linkMap.clear();
        }

        private void queue(Link link) {
            if (!link.isChipLoaded()) {
                linkMap.computeIfAbsent(link.iri(), k -> new ArrayList<>()).add(link);
            }
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
            Condition object;
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
                String propertyKey = slice.propertyKey();

                QueryTree qt = slice.isRange()
                        ? qTree.removeAll(selectedFacets.getRangeSelected(propertyKey))
                        : qTree;

                List<Condition> selected = selectedFacets.getSelected(propertyKey);
                Connective connective = selectedFacets.inferConnective(propertyKey).orElse(slice.defaultConnective());

                List<Map<String, Object>> observations = new ArrayList<>();

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
                            var c = new Condition(property, Operator.EQUALS, v);

                            if (c.value() instanceof Link l && l.iri().equals(queryParams.object)) {
                                // TODO: This check won't be needed if/when we remove facets from resource page.
                                return;
                            }

                            // TODO
                            boolean isSelected = selectedValue != null && !selectedValue.isEmpty()
                                    ? selectedValue.stream().anyMatch(n -> n instanceof Condition c2 && c2.value() instanceof Link l && v instanceof Link l2 && l.iri().equals(l2.iri()))
                                    : selected.contains(c);

                            Consumer<QueryTree> addObservation = alteredTree -> {
                                Map<String, Object> observation = new LinkedHashMap<>();

                                observation.put("totalItems", count);
                                observation.put("view", Map.of(JsonLd.ID_KEY, makeViewFindUrl(alteredTree.toQueryString(), queryParams)));
                                observation.put("object", v instanceof Resource r ? r.description() : v.toString());
                                if (connective == Connective.OR) {
                                    observation.put("_selected", isSelected);
                                } else if (isSelected) {
                                    observation.put("_selected", true);
                                }
                                if (o.subSlices != null && slice.subSlice() != null) {
                                    var sliceByDimension = o.subSlices.getSliceByDimension(List.of(slice.subSlice()), selectedFacets, v, selectedValue);
                                    if (!sliceByDimension.isEmpty()) {
                                        observation.put("sliceByDimension", sliceByDimension);
                                    }
                                }

                                observations.add(observation);

                                if (c.value() instanceof Link l) {
                                    links.add(l);
                                }
                            };

                            if (getSelectedFacets().isRadioButton(propertyKey)) {
                                // unselect others with same property
                                // TODO don't hardcode category if this is what we want
                                // FIXME
                                //List<Node> selected = selectedValue != null ? selectedValue : Collections.emptyList();
                                //addObservation.accept(qt.remove(selected).add(pv));
                                Predicate<Node> f = (Node n) -> n instanceof Condition c2
                                        && c2.selector() instanceof Property p
                                        && p instanceof Property.CoercingSubProperty coercing && coercing.getSuperProperty().name().equals(WORK_CATEGORY);

                                var qt2 = qt.removeAll(qt.findTopNodesByCondition(n -> f.test(n) || n instanceof Or or && or.children().stream().anyMatch(f)));
                                if (selectedValue == null || !selectedValue.contains(c)) {
                                    qt2 = qt2.add(c);
                                }

                                addObservation.accept(qt2);
                                return;
                            }

                            if (isSelected) {
                                selected.stream()
                                        .filter(c::equals)
                                        .findFirst()
                                        .map(qt::remove)
                                        .ifPresent(addObservation);
                            } else {
                                if (selected.isEmpty()) {
                                    addObservation.accept(qt.add(c));
                                } else {
                                    var newSelected = with(new ArrayList<>(selected), l -> l.add(c));
                                    var alteredTree = qt.removeAll(selected)
                                            .add(switch (connective) {
                                                case AND -> new And(newSelected);
                                                case OR -> new Or(newSelected);
                                            });
                                    addObservation.accept(alteredTree);
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
                var s = getSliceByDimension(slices, selectedFacets, null, null);

                /*
                // Move @none to under selected find/identify
                // TODO don't hardcode this if we decide it is what we want
                var none = s.remove(NONE_CATEGORY);
                if (none != null) {
                    var find =  s.get(FIND_CATEGORY);
                    if (find != null) {
                        DocumentUtil.traverse(find, (value, path) -> {
                            if (value instanceof Map m && m.containsKey("_selected") && m.get("_selected").equals(true) && !path.contains(NONE_CATEGORY)) {
                                var newV = new HashMap<>(m);
                                ((Map) newV.computeIfAbsent("sliceByDimension", k -> new HashMap<>())).put(NONE_CATEGORY, none);
                                return new DocumentUtil.Replace(newV);
                            }
                            return DocumentUtil.NOP;
                        });
                    }
                }*/

                return s;
            }

            private Map<String, Object> getSliceByDimension(List<AppParams.Slice> slices, SelectedFacets selectedFacets, Value parentValue, List<Node> selectedValue) {
                Map<String, Object> result = new LinkedHashMap<>();

                slices.forEach(slice -> {
                    var property = slice.getProperty();
                    var propertyKey = slice.propertyKey();

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
                    if (selectedFacets.isRadioButton(propertyKey) && parentValue == null && selectedValue == null) {
                        var values = new HashSet<String>();
                        sliceResult.collectValues(values);

                        // TODO
                        mySelectedValue = qTree.findTopNodesByCondition(node ->
                                (node instanceof Condition c && c.value() instanceof Link link && values.contains(link.iri()))

                        );

//pv.path().expand(whelk.getJsonld()).firstProperty().map(p -> p.equals(property)).orElse(false)
//&& pv.value() instanceof Link link && values.contains(link.iri())

                    }

                    var sliceNode = new LinkedHashMap<>();
                    var observations = sliceResult.getObservations(slice, parentValue, mySelectedValue, selectedFacets);
                    if (!observations.isEmpty() || parentValue != null) {
                        if (slice.isRange()) {
                            sliceNode.put("search", getRangeTemplate(property));
                        }
                        sliceNode.put("dimension", property.name());
                        sliceNode.put("observation", observations);
                        sliceNode.put("maxItems", slice.size());
                        sliceNode.put("_connective", selectedFacets.inferConnective(propertyKey).orElse(slice.defaultConnective()));
                        result.put(property.name(), sliceNode);
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

        private Map<String, Object> getRangeTemplate(Property property) {
            List<Condition> selected = getSelectedFacets().getSelected(property.name());
            String queryKey = property.formattedQueryKey();
            Condition placeholderNode = new FreeText(String.format("{?%s}", queryKey)).asNode();
            String templateQueryString = qTree.removeAll(selected)
                    .add(placeholderNode)
                    .toQueryString();
            String templateUrl = QueryUtil.makeViewFindUrl(templateQueryString, queryParams);

            String selectedMin = "";
            String selectedMax = "";
            if (selected.size() == 1 && selected.getFirst().value() instanceof YearRange yr) {
                selectedMin = yr.min();
                selectedMax = yr.max();
            }

            Map<String, String> mapping = Map.of(
                    "variable", queryKey,
                    Operator.GREATER_THAN_OR_EQUALS.termKey, selectedMin,
                    Operator.LESS_THAN_OR_EQUALS.termKey, selectedMax
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
                List<Node> implied = qTree.findTopNodesByCondition(n -> implies(fa, n, jsonLd));
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
                        if (implies(new And(implied), fa.getParsed(), jsonLd)) {
                            isSelected = true;
                            yield qTree.removeAll(implied);
                        } else {
                            yield qTree.removeAll(implied).add(fa);
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

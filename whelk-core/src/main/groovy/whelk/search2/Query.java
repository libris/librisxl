package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.FreeText;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Literal;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.Value;
import whelk.util.DocumentUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    private final Stats stats;
    private final LinkLoader linkLoader;

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
        queryTree.applySiteFilters(SearchMode.STANDARD_SEARCH, appParams.siteFilters);
        return getEsQueryDsl(getEsQuery(), queryParams.skipStats ? Map.of() : getEsAggQuery());
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

    protected Map<String, Object> getEsQuery(QueryTree queryTree, Collection<String> rulingTypes) {
        var esQuery = queryTree.toEs(whelk.getJsonld(), esSettings.mappings, queryParams.boostFields, rulingTypes);
        return addBoosts(esQuery, queryParams.esScoreFunctions);
    }

    protected Map<String, Object> getEsAggQuery(Collection<String> rulingTypes) {
        return Aggs.buildAggQuery(appParams.statsRepr.sliceList(),
                whelk.getJsonld(),
                rulingTypes,
                esSettings.mappings);
    }

    protected Map<String, Object> getEsQueryDsl(Map<String, Object> query, Map<String, Object> aggs) {
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

        queryDsl.put("track_total_hits", true);

        if (queryParams.debug.contains(QueryParams.Debug.ES_SCORE)) {
            queryDsl.put("explain", true);
            // Scores won't be calculated when also using sort unless explicitly asked for
            queryDsl.put("track_scores", true);
            queryDsl.put("fields", List.of("*"));
        }

        return queryDsl;
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
        }

        if (!getQueryResult().spell.isEmpty()) {
            view.put("_spell", Spell.buildSpellSuggestions(getQueryResult(), queryTree, queryParams));
        }

        view.put("maxItems", esSettings.maxItems());

        if (queryParams.debug.contains(QueryParams.Debug.ES_QUERY)) {
            view.put(QueryParams.ApiParams.DEBUG, Map.of(QueryParams.Debug.ES_QUERY, getEsQueryDsl()));
        }

        linkLoader.queue(stats.getLinks());
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

    private Map<String, Object> getEsAggQuery() {
        return getEsAggQuery(queryTree.collectRulingTypes(whelk.getJsonld()));
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
            var buckets = collectBuckets();
            var rangeProps = appParams.statsRepr.getRangeProperties();
            return buildSliceByDimension(buckets, rangeProps);
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

        private Map<String, Object> buildSliceByDimension(Map<Property, Map<PathValue, Integer>> propToBuckets, Set<String> rangeProps) {
            Map<String, AppParams.Slice> sliceByPropertyKey = appParams.statsRepr.getSliceByPropertyKey();

            Map<String, Object> sliceByDimension = new LinkedHashMap<>();

            propToBuckets.forEach((property, buckets) -> {
                var propertyKey = property.name();
                var sliceNode = new LinkedHashMap<>();
                var isRange = rangeProps.contains(propertyKey);
                var qTree = isRange
                        ? queryTree.removeTopLevelNodesByCondition(node -> node instanceof PathValue pv
                            && pv.hasEqualProperty(property)
                            && Operator.rangeOperators().contains(pv.operator()))
                        : queryTree;
                var observations = getObservations(buckets, qTree);
                if (!observations.isEmpty()) {
                    if (isRange) {
                        sliceNode.put("search", getRangeTemplate(property));
                    }
                    sliceNode.put("dimension", propertyKey);
                    sliceNode.put("observation", observations);
                    sliceNode.put("maxItems", sliceByPropertyKey.get(propertyKey).size());
                    sliceByDimension.put(propertyKey, sliceNode);
                }
            });

            return sliceByDimension;
        }

        private List<Map<String, Object>> getObservations(Map<PathValue, Integer> buckets, QueryTree qt) {
            List<Map<String, Object>> observations = new ArrayList<>();

            buckets.forEach((pv, count) -> {
                Map<String, Object> observation = new LinkedHashMap<>();
                boolean queried = qt.topLevelContains(pv) || (pv.value() instanceof Link l && l.iri().equals(queryParams.object));
                if (!queried) {
                    observation.put("totalItems", count);
                    var url = makeFindUrlNoOffset(qt.addTopLevelNode(pv), queryParams);
                    observation.put("view", Map.of(JsonLd.ID_KEY, url));
                    observation.put("object", pv.value().description());
                    if (pv.value() instanceof Link l) {
                        links.add(l);
                    }
                    observations.add(observation);
                }
            });

            return observations;
        }

        private Map<String, Object> getRangeTemplate(Property property) {
            var GtLtNodes = queryTree.getTopLevelNodesOfType(PathValue.class).stream()
                    .filter(pv -> pv.hasEqualProperty(property))
                    .filter(pv -> switch (pv.operator()) {
                        case EQUALS, NOT_EQUALS -> false;
                        case GREATER_THAN_OR_EQUALS, GREATER_THAN, LESS_THAN_OR_EQUALS, LESS_THAN -> true;
                    })
                    .filter(pv -> pv.value() instanceof Literal l && l.isNumeric())
                    .toList();

            String min = null;
            String max = null;

            for (var pv : GtLtNodes) {
                var orEquals = pv.toOrEquals();
                if (orEquals.operator() == Operator.GREATER_THAN_OR_EQUALS && min == null) {
                    min = orEquals.value().raw();
                } else if (orEquals.operator() == Operator.LESS_THAN_OR_EQUALS && max == null) {
                    max = orEquals.value().raw();
                } else {
                    // Not a proper range, reset and abort
                    min = null;
                    max = null;
                    break;
                }
            }

            var tree = queryTree.removeTopLevelNodesByCondition(node -> node instanceof PathValue pv
                    && pv.hasEqualProperty(property));

            Map<String, Object> template = new LinkedHashMap<>();

            var placeholderNode = new FreeText(String.format("{?%s}", property.name()));
            var templateQueryString = tree.addTopLevelNode(placeholderNode).toQueryString();
            var templateUrl = makeFindUrlNoOffset(templateQueryString, queryParams);
            template.put("template", templateUrl);

            var mapping = new LinkedHashMap<>();
            mapping.put("variable", property.name());
            mapping.put(Operator.GREATER_THAN_OR_EQUALS.termKey, Objects.toString(min, ""));
            mapping.put(Operator.LESS_THAN_OR_EQUALS.termKey, Objects.toString(max, ""));
            template.put("mapping", mapping);

            return template;
        }

        private List<Map<String, Object>> getBoolFilters() {
            List<Map<String, Object>> results = new ArrayList<>();

            for (var of : appParams.siteFilters.optionalFilters()) {
                QueryTree newTree;
                boolean isSelected;
                Node filterNode = of.filter().getActive();

                if (queryTree.topLevelContains(filterNode)) {
                    newTree = queryTree.removeTopLevelNode(filterNode);
                    isSelected = true;
                } else {
                    newTree = queryTree.addTopLevelNode(filterNode);
                    isSelected = false;
                }

                Map<String, Object> res = new LinkedHashMap<>();
                // TODO: fix form
                res.put("totalItems", 0);
                res.put("object", of.filter().description());
                res.put("view", Map.of(JsonLd.ID_KEY, makeFindUrlNoOffset(newTree, queryParams)));
                res.put("_selected", isSelected);

                results.add(res);
            }

            return results;
        }
    }
}

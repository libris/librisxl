package whelk.search2.esquery;

import whelk.JsonLd;
import whelk.search2.AppParams;
import whelk.search2.Operator;
import whelk.search2.QueryParams;
import whelk.search2.SelectedFacets;
import whelk.search2.Sort;
import whelk.search2.Spell;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.QueryTreeExpander;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.selector.Selector;
import whelk.search2.querytree.value.Any;
import whelk.search2.querytree.value.Link;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static whelk.component.ElasticSearch.SystemFields.ES_ID;
import static whelk.search2.Disambiguate.getPropertyByKey;
import static whelk.search2.QueryUtil.castToStringObjectMap;
import static whelk.search2.esquery.ESMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.esquery.ESMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.esquery.ESMappings.KEYWORD;

public record ESQueryDefinition(QueryTree tree,
                                ESSettings settings,
                                QueryParams queryParams,
                                JsonLd jsonLd,
                                Collection<String> rdfSubjectTypes,
                                AggsDefinition aggs,
                                PAggsDefinition pAggs)
{
    public static final String NESTED_AGG_NAME = "n";
    public static final String REVERSE_NESTED_AGG_NAME = "r";

    private static final String FIND_CATEGORY = "librissearch:findCategory";
    private static final String IDENTIFY_CATEGORY = "librissearch:identifyCategory";
    private static final String NONE_CATEGORY = "librissearch:noneCategory";

    public record AggsDefinition(List<AppParams.Slice> slices,
                                 SelectedFacets selectedFacets) {
    }

    public record PAggsDefinition(Link object, List<PredicateDefinition> predicates) {
    }

    public record PredicateDefinition(Property p, List<String> allowedSubjectTypes) {
    }

    public Map<String, Object> dsl() {
        boolean includeAggsQuery = aggs != null;

        ESQueryTree esQueryTree = (includeAggsQuery
                ? aggs.selectedFacets().flagMultiOrRadioSelectedForPostFilter(tree)
                : tree)
                .expand(jsonLd)
                .toEsQuery(settings)
                .add(settings.boost().getScoreFunctions());

        Map<String, Object> dsl = includeAggsQuery
                ? esQueryTree.dslWithPostFilter()
                : esQueryTree.dsl();

        dsl.put("size", queryParams.limit);
        dsl.put("from", queryParams.offset);
        dsl.put("sort", buildSortClauses(queryParams.sortBy, rdfSubjectTypes, settings.mappings(), jsonLd));

        if (queryParams.spell.suggest && settings.mappings().spellFieldExists()) {
            var spellQuery = Spell.getSpellQuery(tree);
            if (spellQuery.isPresent()) {
                if (queryParams.spell.suggestOnly) {
                    return Map.of("suggest", spellQuery.get());
                } else {
                    dsl.put("suggest", spellQuery.get());
                }
            }
        }

        dsl.put("track_total_hits", true);

        if (queryParams.debug.contains(QueryParams.Debug.ES_SCORE)) {
            dsl.put("explain", true);
            // Scores won't be calculated when also using sort unless explicitly asked for
            dsl.put("track_scores", true);
            dsl.put("fields", List.of("*"));
        }

        if (!settings.sourceExcludes().isEmpty()) {
            dsl.put("_source", Map.of("excludes", settings.sourceExcludes()));
        }

        if (includeAggsQuery) {
            dsl.put("aggs", buildAggsQuery());
        }

        if (pAggs != null) {
            Map<String, Object> aggsQuery = castToStringObjectMap(dsl.computeIfAbsent("aggs", _ -> new LinkedHashMap<>()));
            aggsQuery.putAll(buildPAggsQuery());
        }

        return dsl;
    }

    public Map<String, Object> buildAggsQuery() {
        Map<String, Object> query = new LinkedHashMap<>();

        if (aggs.slices().isEmpty()) {
            query.put(JsonLd.TYPE_KEY, Map.of("terms", Map.of("field", JsonLd.TYPE_KEY)));
        } else {
            for (AppParams.Slice slice : aggs.slices()) {
                addSliceToAggsQuery(query, slice);
            }
        }

        return query;
    }

    public Map<String, Object> buildPAggsQuery() {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = new HashMap<>();

        pAggs.predicates().forEach(pred -> {
            Condition pCondition = new Condition(pred.p(), Operator.EQUALS, pAggs.object());
            Map<String, Object> filter = toEs(pCondition, pred.allowedSubjectTypes()).dsl();
            filters.put(pred.p().name(), filter);
        });

        if (!filters.isEmpty()) {
            query.put(QueryParams.ApiParams.PREDICATES, Map.of("filters", Map.of("filters", filters)));
        }

        return query;
    }

    private void addSliceToAggsQuery(Map<String, Object> query, AppParams.Slice slice) {
        String pKey = slice.propertyKey();

        SelectedFacets selectedFacets = aggs.selectedFacets();

        if (!selectedFacets.isSelectable(pKey)) {
            return;
        }

        Property property = slice.getProperty();

        if (!slice.getShowIf().isEmpty()
                && selectedFacets.isInactive(FIND_CATEGORY)
                && selectedFacets.isInactive(IDENTIFY_CATEGORY)
                && selectedFacets.isInactive(NONE_CATEGORY)) {
            // Enable @none facet if find/identify/@none in query
            // TODO don't hardcode this if we decide it is what we want
            return;
        }

        if (property instanceof Property.RestrictedSubProperty && !property.hasIndexKey()) {
            // TODO: E.g. author (combining contribution.role and contribution.agent)
            throw new RuntimeException("Can't handle combined fields in aggs query");
        }

        QueryTreeExpander.expandProperty(property, jsonLd, rdfSubjectTypes, false)
                .forEach(selector -> {
                    String field = selector.esField();
                    if (settings.mappings().hasFourDigitsKeywordField(field)) {
                        field = String.format("%s%s", field, FOUR_DIGITS_KEYWORD_SUFFIX);
                    } else if (settings.mappings().hasKeywordSubfield(field)) {
                        field = String.format("%s.%s", field, KEYWORD);
                    } else if (property.isObjectProperty() && !property.isVocabTerm() && !property.isType()) {
                        field = String.format("%s.%s", field, JsonLd.ID_KEY);
                    }
                    if (!settings.mappings().isAggregatable(field)) {
                        return;
                    }
                    Optional<String> nestedStem = ESQueryTreeBuilder.getNestedStem(field, settings.mappings());
                    Map<String, Object> aggQuery = nestedStem.isPresent()
                            ? buildNestedAggQuery(field, slice, nestedStem.get())
                            : buildCoreAqqQuery(field, slice);
                    Map<String, List<Condition>> mSelected = selectedFacets.isMultiOrRadio(pKey)
                            ? with(new HashMap<>(selectedFacets.getAllMultiOrRadioSelected()), m -> {
                        m.remove(pKey);
                        // FIXME
                        if (slice.parentSlice() != null) {
                            m.remove(slice.parentSlice().propertyKey());
                        }
                        if (slice.subSlice() != null) {
                            m.remove(slice.subSlice().propertyKey());
                        }
                        // TODO don't hardcode this if we decide it is what we want
                        if (FIND_CATEGORY.equals(pKey) || IDENTIFY_CATEGORY.equals(pKey)) {
                            m.remove(NONE_CATEGORY);
                        }
                        //if ("_categoryByCollection.@none".equals(pKey)) {
                        //    m.remove("_categoryByCollection.find");
                        //    m.remove("_categoryByCollection.identify");
                        //}
                    })
                            : selectedFacets.getAllMultiOrRadioSelected();

                    Node multiSelectedTree = buildMultiSelectedTree(mSelected.values());
                    Map<String, Object> filter = toEs(multiSelectedTree, rdfSubjectTypes).dsl();

                    query.put(field, filterWrap(aggQuery, property.name(), filter));
                });
    }

    private Map<String, Object> buildCoreAqqQuery(String field, AppParams.Slice slice) {
        return buildCoreAqqQuery(field, slice, false);
    }

    private Map<String, Object> buildCoreAqqQuery(String field, AppParams.Slice slice, boolean isInsideNested) {
        var q = Map.of("terms",
                Map.of("field", field,
                        "size", slice.size(),
                        "order", buildBucketOrder(slice.bucketSort())));

        if (slice.subSlice() != null) {
            q = new LinkedHashMap<>(q);

            Map<String, Object> query = new LinkedHashMap<>();
            addSliceToAggsQuery(query, slice.subSlice());
            q.put("aggs", query);
        }
        else if (slice.shouldCountTopLevelDocs() && isInsideNested) {
            // count the number of top-level documents instead of the number of nested docs
            // for example multiple holdings with the same organization (heldBy.isPartOf.@id)
            // isInsideNested - if the nested field doesn't exist, i.e. this won't be a nested agg
            // we shouldn't generate the reverse nested either because that will be an invalid query
            q = new LinkedHashMap<>(q);
            Map<String, Object> reverse = Map.of(
                    REVERSE_NESTED_AGG_NAME, Map.of(
                            "reverse_nested", Collections.emptyMap(),
                            "aggs", Map.of(
                                    REVERSE_NESTED_AGG_NAME, Map.of(
                                            "cardinality", Map.of(
                                                    "field", ES_ID
                                            )
                                    )
                            )
                    )
            );
            q.put("aggs", reverse);
        }

        return castToStringObjectMap(q);
    }

    private static Map<String, String> buildBucketOrder(Sort.Key k) {
        // count -> _count, key -> _key
        return Map.of("_" + k.key(), k.order().name());
    }

    private Map<String, Object> buildNestedAggQuery(String field, AppParams.Slice slice, String nestedStem) {
        return Map.of("nested", Map.of("path", nestedStem),
                "aggs", Map.of(NESTED_AGG_NAME, buildCoreAqqQuery(field, slice, true)));
    }

    private ESNode toEs(Node node, Collection<String> subjectTypes) {
        Node expanded = QueryTreeExpander.expand(node, jsonLd, subjectTypes);
        return ESQueryTreeBuilder.buildFrom(expanded, settings);
    }

    private static Node buildMultiSelectedTree(Collection<? extends List<? extends Node>> multiSelected) {
        if (multiSelected.isEmpty()) {
            return new Any.EmptyString().asNode();
        }

        List<Node> orGrouped = multiSelected.stream()
                .map(selected -> selected.size() > 1
                        ? new Or(selected)
                        : selected.getFirst())
                .toList();

        return orGrouped.size() == 1 ? orGrouped.getFirst() : new And(orGrouped);
    }

    private static Map<String, Object> filterWrap(Map<String, Object> aggs, String property, Map<String, Object> filter) {
        return Map.of("aggs", Map.of(property, aggs),
                "filter", filter);
    }

    private static List<Map<String, Object>> buildSortClauses(Sort sort,
                                                              Collection<String> rdfSubjectTypes,
                                                              ESMappings mappings,
                                                              JsonLd jsonLd) {
        return sort.keys().stream()
                .map(k -> buildSortClause(k, rdfSubjectTypes, mappings, jsonLd))
                .toList();
    }

    private static Map<String, Object> buildSortClause(Sort.Key k,
                                                       Collection<String> rdfSubjectTypes,
                                                       ESMappings mappings,
                                                       JsonLd jsonLd) {
        String field = getSortField(k.key(), rdfSubjectTypes, mappings, jsonLd);
        Map<String, Object> sort = new LinkedHashMap<>();
        sort.put("order", k.order());
        sort.put("missing", "_last");
        ESQueryTreeBuilder.getNestedStem(field, mappings)
                .filter(stem -> !mappings.isNestedIncludeInParentField(stem))
                .ifPresent(stem -> sort.put("nested", Map.of("path", stem)));
        return Map.of(field, sort);
    }

    private static String getSortField(String key,
                                       Collection<String> rdfSubjectTypes,
                                       ESMappings mappings,
                                       JsonLd jsonLd) {
        Optional<Property> sortKey = getSortKey(key, jsonLd);
        if (sortKey.isPresent() && sortKey.get().hasIndexKey()) {
            return sortKey.get().esField();
        }
        String field = sortKey.map(p -> expandSortKey(p, key, rdfSubjectTypes, jsonLd)).orElse(key);
        return withSuffix(field, mappings);
    }

    private static Optional<Property> getSortKey(String key, JsonLd jsonLd) {
        try {
            Property property = getPropertyByKey(key, jsonLd);
            return property.isSortKey() ? Optional.of(property) : Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static String expandSortKey(Property sortKey, String key, Collection<String> rdfSubjectTypes, JsonLd jsonLd) {
        List<Selector> expanded = QueryTreeExpander.expandProperty(sortKey, jsonLd, rdfSubjectTypes, false);
        // TODO: Validate sort keys in general and report invalid ones to the client instead of passing them on
        //  to Elastic. For now, a sort key that is ambiguous (e.g. [p, instanceOf.p]) or incompatible with the
        //  queried types is passed on as is, just like any other unrecognized sort key.
        return expanded.size() == 1
                ? expanded.getFirst().esField()
                : key;
    }

    private static String withSuffix(String sortField, ESMappings mappings) {
        if (mappings.hasFourDigitsShortField(sortField)) {
            return String.format("%s%s", sortField, FOUR_DIGITS_SHORT_SUFFIX);
        }
        else if (mappings.hasKeywordSubfield(sortField)) {
            return String.format("%s.%s", sortField, KEYWORD);
        } else {
            return sortField;
        }
    }

    private static <T> T with(T t, Consumer<T> f) {
        f.accept(t);
        return t;
    }
}
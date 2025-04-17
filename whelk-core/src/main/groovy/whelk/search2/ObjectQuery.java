package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryParams.ApiParams.PREDICATES;
import static whelk.search2.QueryUtil.makeFindUrl;

public class ObjectQuery extends Query {
    protected Link object;
    protected List<Property> curatedPredicates;

    public ObjectQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
        this.object = loadObject();
        this.curatedPredicates = loadCuratedPredicates();
    }

    @Override
    protected Object doGetEsQueryDsl() {
        queryTree.applySiteFilters(SearchMode.OBJECT_SEARCH, appParams.siteFilters);
        queryTree.applyObjectFilter(object.iri());

        List<String> rulingTypes = queryTree.collectRulingTypes(whelk.getJsonld());
        List<String> inferredSubjectTypes = rulingTypes.isEmpty() ? inferSubjectTypes() : List.of();

        var mainQuery = getEsQuery(queryTree, inferredSubjectTypes);

        if (queryParams.skipStats) {
            return getEsQueryDsl(mainQuery, Map.of());
        }

        var aggQuery = getEsAggQuery(inferredSubjectTypes);
        aggQuery.putAll(getPAggQuery(rulingTypes));

        return getEsQueryDsl(mainQuery, aggQuery);
    }

    @Override
    protected List<Map<String, Object>> predicateLinks() {
        if (curatedPredicates == null) {
            return Collections.emptyList();
        }

        var result = new ArrayList<Map<String, Object>>();

        Map<String, Integer> counts = getQueryResult().pAggs.stream()
                .collect(Collectors.toMap(Aggs.Bucket::value, Aggs.Bucket::count));

        curatedPredicates.stream()
                .map(Property::name)
                .forEach(p -> {
                    if (!counts.containsKey(p)) {
                        return;
                    }

                    int count = counts.get(p);

                    if (count > 0) {
                        Map<String, String> params = queryParams.getNonQueryParamsNoOffset();
                        params.put(PREDICATES, p);
                        result.add(Map.of(
                                "totalItems", count,
                                "view", Map.of(JsonLd.ID_KEY, makeFindUrl(params)),
                                "object", whelk.getJsonld().vocabIndex.get(p),
                                "_selected", queryParams.predicates.contains(p)
                        ));
                    }
                });

        return result;
    }

    protected Map<String, Object> getPAggQuery(List<String> rulingTypes) {
        return Aggs.buildPAggQuery(object,
                curatedPredicates,
                whelk.getJsonld(),
                rulingTypes,
                this::getNestedPath);
    }

    private List<String> inferSubjectTypes() {
        var objectSuperTypes = whelk.getJsonld().getSuperClasses(object.getType());
        return Stream.concat(Stream.of(object.getType()), objectSuperTypes.stream())
                .map(whelk.getJsonld()::getInRange)
                .flatMap(Set::stream)
                .map(pKey -> new Property(pKey, whelk.getJsonld()))
                .map(Property::domain)
                .flatMap(List::stream)
                .distinct()
                .toList();
    }


    private Link loadObject() throws InvalidQueryException {
        var object = queryParams.object;
        if (queryParams.object != null) {
            Map<String, Object> thing = QueryUtil.loadThing(queryParams.object, whelk);
            if (!thing.isEmpty()) {
                return new Link(queryParams.object, thing);
            }
        }
        throw new InvalidQueryException("No resource with id " + object + " was found");
    }

    private List<Property> loadCuratedPredicates() {
        return Stream.concat(Stream.of(object.getType()), whelk.getJsonld().getSuperClasses(object.getType()).stream())
                .filter(appParams.relationFilters::containsKey)
                .findFirst().map(appParams.relationFilters::get)
                .map(predicates -> predicates.stream().map(p -> new Property(p, whelk.getJsonld())).toList())
                .orElse(Collections.emptyList());
    }
}

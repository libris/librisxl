package whelk.search2;

import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PredicateObjectQuery extends ObjectQuery {
    public PredicateObjectQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
    }

    @Override
    protected Object doGetEsQueryDsl() {
        applySiteFilters(SearchMode.PREDICATE_OBJECT_SEARCH);

        QueryTree queryTreeCopy = queryTree.copy();

        List<Property> predicates = queryParams.predicates.stream()
                .map(p -> new Property(p, whelk.getJsonld()))
                .toList();
        queryTree.applyPredicateObjectFilter(predicates, object.iri());

        List<String> rulingTypes = queryTree.collectRulingTypes(whelk.getJsonld());
        List<String> inferredSubjectTypes = rulingTypes.isEmpty() ? inferSubjectTypes(predicates) : rulingTypes;

        var mainQuery = getEsQuery(queryTree, inferredSubjectTypes);

        if (queryParams.skipStats) {
            return getEsQueryDsl(mainQuery);
        }

        var aggQuery = getEsAggQuery(inferredSubjectTypes);
        var postFilter = getPostFilter(inferredSubjectTypes);

        var mainQueryDsl = getEsQueryDsl(mainQuery, aggQuery, postFilter);
        var pAggQueryDsl = getPAggQueryDsl(getEsQuery(queryTreeCopy, inferredSubjectTypes), getPAggQuery(rulingTypes));

        return List.of(mainQueryDsl, pAggQueryDsl);
    }

    private Map<String, Object> getPAggQueryDsl(Map<String, Object> query, Map<String, Object> pAggQuery) {
        var queryDsl = new LinkedHashMap<String, Object>();

        queryDsl.put("query", query);
        queryDsl.put("size", 0);
        queryDsl.put("from", 0);
        queryDsl.put("aggs", pAggQuery);
        queryDsl.put("track_total_hits", true);

        return queryDsl;
    }

    private static List<String> inferSubjectTypes(List<Property> predicates) {
        return predicates.stream().map(Property::domain).flatMap(List::stream).distinct().toList();
    }
}

package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.And;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.Term;
import whelk.search2.querytree.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryParams.ApiParams.CUSTOM_SITE_FILTER;
import static whelk.search2.QueryParams.ApiParams.OBJECT;
import static whelk.search2.QueryParams.ApiParams.PREDICATES;
import static whelk.search2.QueryParams.ApiParams.SORT;
import static whelk.search2.QueryUtil.makeFindUrl;

public class ObjectQuery extends Query {
    protected final Link object;
    private final List<Property> curatedPredicates;

    public ObjectQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
        this.object = loadObject();
        this.curatedPredicates = loadCuratedPredicates();
    }

    @Override
    protected Object doGetEsQueryDsl() {
        QueryTree queryTree = getFullQueryTree().add(objectFilter());

        JsonLd ld = whelk.getJsonld();

        List<String> givenSubjectTypes = queryTree.getRdfSubjectTypesList();

        Set<String> inferredSubjectTypes = new HashSet<>();
        Map<Property, List<String>> predicateToSubjectTypes = new HashMap<>();

        curatedPredicates.forEach(p -> {
            List<String> subjects = queryTree.getRdfSubjectTypesList().stream()
                    .filter(t -> p.appearsOnType(t, ld) || p.indirectlyAppearsOnType(t, ld))
                    .toList();
            if (!subjects.isEmpty()) {
                predicateToSubjectTypes.put(p, subjects);
            } else {
                inferredSubjectTypes.addAll(p.domain());
                predicateToSubjectTypes.put(p, p.domain());
            }
        });

        Map<String, Object> mainQuery;

        if (!inferredSubjectTypes.isEmpty()) {
            List<Node> altTrees = new ArrayList<>();
            altTrees.add(queryTree.tree());
            inferredSubjectTypes.stream()
                    .map(t -> new And(List.of(new Type(t, ld), objectFilter())))
                    .map(QueryTree::new)
                    .map(this::getFullQueryTree)
                    .map(QueryTree::tree)
                    .forEach(altTrees::add);
            mainQuery = getEsQuery(new QueryTree(new Or(altTrees)));
        } else {
            mainQuery = getEsQuery(queryTree);
        }

        if (queryParams.skipStats) {
            return getEsQueryDsl(mainQuery, getPAggQuery(predicateToSubjectTypes));
        }

        List<String> subjectTypes = Stream.concat(givenSubjectTypes.stream(), inferredSubjectTypes.stream()).toList();
        var aggQuery = getEsAggQuery(subjectTypes);
        var postFilter = getPostFilter(subjectTypes);
        aggQuery.putAll(getPAggQuery(predicateToSubjectTypes));

        return getEsQueryDsl(mainQuery, aggQuery, postFilter);
    }

    @Override
    protected List<Map<String, Object>> predicateLinks() {
        var result = new ArrayList<Map<String, Object>>();

        Map<String, Integer> counts = getQueryResult().pAggs.stream()
                .collect(Collectors.toMap(QueryResult.Bucket::value, QueryResult.Bucket::count));

        curatedPredicates.stream()
                .map(Property::name)
                .forEach(p -> {
                    if (!counts.containsKey(p)) {
                        return;
                    }

                    int count = counts.get(p);

                    if (count > 0) {
                        Map<String, String> params = queryParams.getCustomParamsMap(List.of(CUSTOM_SITE_FILTER, SORT, OBJECT));
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

    private PathValue objectFilter() {
        return new PathValue("_links", Operator.EQUALS, new Term(object.iri()));
    }

    private Map<String, Object> getPAggQuery(Map<Property, List<String>> predicateToSubjectTypes) {
        return buildPAggQuery(object, predicateToSubjectTypes, whelk.getJsonld(), esSettings);
    }

    private static Map<String, Object> buildPAggQuery(Link object,
                                                      Map<Property, List<String>> predicateToSubjectTypes,
                                                      JsonLd jsonLd,
                                                      ESSettings esSettings)
    {
        Map<String, Object> query = new LinkedHashMap<>();

        var filters = new HashMap<>();

        predicateToSubjectTypes.forEach((p, subjects) -> {
            var filter = new PathValue(p, Operator.EQUALS, object)
                    .expand(jsonLd, subjects)
                    .toEs(esSettings);
            filters.put(p.name(), filter);
        });

        if (!filters.isEmpty()) {
            query.put(QueryParams.ApiParams.PREDICATES, Map.of("filters", Map.of("filters", filters)));
        }

        return query;
    }

    private Link loadObject() throws InvalidQueryException {
        var o = queryParams.object;
        if (o != null) {
            Map<String, Object> thing = QueryUtil.loadThing(o, whelk);
            if (!thing.isEmpty()) {
                return new Link(o, thing);
            }
        }
        throw new InvalidQueryException("No resource with id " + object + " was found");
    }

    private List<Property> loadCuratedPredicates() {
        return appParams.filters.relationFilter().stream()
                .filter(r -> whelk.getJsonld().isSubClassOf(object.getType(), r.objectType()))
                .findFirst()
                .map(AppParams.RelationFilter::predicates)
                .map(predicates -> predicates.stream().map(p -> new Property(p, whelk.getJsonld())).toList())
                .orElse(Collections.emptyList());
    }
}

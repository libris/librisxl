package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.esquery.ESQuery;
import whelk.search2.esquery.ESSettings;
import whelk.search2.esquery.ESQueryDefinition;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.value.Link;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.value.Term;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.component.ElasticSearch.SystemFields.LINKS;
import static whelk.search2.QueryParams.ApiParams.QUERY;
import static whelk.search2.QueryUtil.makeFindUrl;

public class ObjectQuery extends Query {
    protected final Link object;
    private final List<Property> curatedPredicates;

    public ObjectQuery(QueryParams queryParams, AppParams appParams, ResourceLookup resourceLookup, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, resourceLookup, esSettings, whelk);
        this.object = loadObject();
        this.curatedPredicates = loadCuratedPredicates();
    }

    @Override
    protected ESQuery getEsQuery() {
        return new ESQuery(prepareEsQuery().dsl(), List.of(), whelk.elastic);
    }

    @Override
    protected List<Map<String, Object>> predicateLinks() {
        var result = new ArrayList<Map<String, Object>>();

        Map<String, Integer> counts = getQueryResult().pAggs.stream()
                .collect(Collectors.toMap(QueryResult.Bucket::value, QueryResult.Bucket::count));

        curatedPredicates.forEach(p -> {
            if (!counts.containsKey(p.name())) {
                return;
            }

            int count = counts.get(p.name());

            if (count > 0) {
                var q = new Condition(p, p.isPreferLike() ? Operator.LIKE : Operator.EQUALS, object).toQueryString();
                result.add(Map.of(
                        "totalItems", count,
                        "view", Map.of(JsonLd.ID_KEY, makeFindUrl(Map.of(QUERY, q))),
                        "predicate", whelk.getJsonld().vocabIndex.get(p.name()),
                        "object", object.description()
                ));
            }
        });

        return result;
    }

    private ESQueryDefinition prepareEsQuery() {
        JsonLd ld = whelk.getJsonld();

        QueryTree queryTree = getFullQueryTree().add(objectFilter());

        List<String> givenSubjectTypes = queryTree.getRdfSubjectType().typeNames();

        Set<String> inferredSubjectTypes = new HashSet<>();
        List<ESQueryDefinition.PredicateDefinition> predicateDefs = new ArrayList<>();

        for (Property p : curatedPredicates) {
            List<String> compatibleGivenSubjectTypes = givenSubjectTypes.stream()
                    .filter(t -> p.appearsOnType(t, ld) || p.indirectlyAppearsOnType(t, ld))
                    .toList();
            if (!compatibleGivenSubjectTypes.isEmpty()) {
                predicateDefs.add(new ESQueryDefinition.PredicateDefinition(p, compatibleGivenSubjectTypes));
            } else {
                inferredSubjectTypes.addAll(p.domain());
                predicateDefs.add(new ESQueryDefinition.PredicateDefinition(p, p.domain()));
            }
        }

        if (!inferredSubjectTypes.isEmpty()) {
            List<Node> altTrees = new ArrayList<>();
            altTrees.add(queryTree.tree());
            inferredSubjectTypes.stream()
                    .map(t -> new And(List.of(new Condition.Type(t, ld), objectFilter())))
                    .map(QueryTree::new)
                    .map(this::getFullQueryTree)
                    .map(QueryTree::tree)
                    .forEach(altTrees::add);
            queryTree = new QueryTree(new Or(altTrees));
        }

        ESQueryDefinition.AggsDefinition aggsDefinition = null;
        if (queryParams.stats.on) {
            List<String> subjectTypes = Stream.concat(givenSubjectTypes.stream(), inferredSubjectTypes.stream()).toList();
            aggsDefinition = new ESQueryDefinition.AggsDefinition(appParams.sliceList, getSelectedFacets(), subjectTypes);
        }

        ESQueryDefinition.PAggsDefinition pAggsDefinition = new ESQueryDefinition.PAggsDefinition(object, predicateDefs);

        return new ESQueryDefinition(queryTree, esSettings, queryParams, ld, aggsDefinition, pAggsDefinition);
    }

    private Condition objectFilter() {
        return new Condition(LINKS, Operator.EQUALS, new Term(object.iri()));
    }

    private Link loadObject() throws InvalidQueryException {
        var o = queryParams.object;
        if (o != null) {
            Map<String, Object> thing = QueryUtil.loadThing(o, whelk);
            if (!thing.isEmpty()) {
                var chip = QueryUtil.castToStringObjectMap(whelk.jsonld.toChip(thing));
                return new Link(o, chip);
            }
        }
        throw new InvalidQueryException("No resource with id " + o + " was found");
    }

    private List<Property> loadCuratedPredicates() {
        return appParams.filters.relationFilter().stream()
                .filter(r -> whelk.getJsonld().isSubClassOf(object.getType(), r.objectType()))
                .findFirst()
                .map(AppParams.RelationFilter::predicates)
                .map(predicates -> predicates.stream().map(p -> Property.getProperty(p, whelk.getJsonld())).toList())
                .orElse(Collections.emptyList());
    }
}

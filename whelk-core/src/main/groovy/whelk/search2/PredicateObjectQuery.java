package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.esquery.ESQuery;
import whelk.search2.esquery.ESSettings;
import whelk.search2.esquery.ESQueryDefinition;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PredicateObjectQuery extends ObjectQuery {
    public PredicateObjectQuery(QueryParams queryParams, AppParams appParams, ResourceLookup resourceLookup, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, resourceLookup, esSettings, whelk);
    }

    @Override
    protected ESQuery getEsQuery() {
        return new ESQuery(prepareEsQuery().dsl(), List.of(), whelk.elastic);
    }

    private ESQueryDefinition prepareEsQuery() {
        JsonLd ld = whelk.getJsonld();

        Map<String, List<Property>> predicatesByInferredSubjectType = new HashMap<>();
        List<Property> noDomain = new ArrayList<>();
        predicates().forEach(property -> {
            if (property.domain().isEmpty()) {
                noDomain.add(property);
            }
            property.domain().forEach(domain ->
                    predicatesByInferredSubjectType.computeIfAbsent(domain, k -> new ArrayList<>())
                            .add(property)
            );
        });

        QueryTree queryTree;
        if (predicatesByInferredSubjectType.isEmpty()) {
            queryTree = getFullQueryTree().add(predicateObjectFilter(predicates()));
        } else {
            List<Node> altTrees = new ArrayList<>();
            if (!noDomain.isEmpty()) {
                altTrees.add(predicateObjectFilter(noDomain));
            }
            predicatesByInferredSubjectType.forEach((type, preds) ->
                altTrees.add(new And(List.of(new Condition.Type(type, ld), predicateObjectFilter(preds))))
            );
            queryTree = new QueryTree(altTrees.size() == 1 ? altTrees.getFirst() : new Or(altTrees))
                    .merge(getFullQueryTree(), ld);
        }

        ESQueryDefinition.AggsDefinition aggsDefinition = null;
        if (queryParams.stats.on) {
            Set<String> subjectTypes = Stream.concat(queryTree.getRdfSubjectType().typeNames().stream(),
                            predicatesByInferredSubjectType.keySet().stream())
                    .collect(Collectors.toSet());
            aggsDefinition = new ESQueryDefinition.AggsDefinition(appParams.sliceList, getSelectedFacets(), subjectTypes);
        }

        return new ESQueryDefinition(queryTree, esSettings, queryParams, ld, aggsDefinition, null);
    }

    private Node predicateObjectFilter(Collection<Property> predicates) {
        var preds = predicates.stream()
                .map(p -> new Condition(p, Operator.EQUALS, object))
                .toList();
        return preds.size() == 1 ? preds.getFirst() : new Or(preds);
    }

    private List<Property> predicates() {
        return queryParams.predicates.stream().map(p -> Property.getProperty(p, whelk.getJsonld())).toList();
    }
}

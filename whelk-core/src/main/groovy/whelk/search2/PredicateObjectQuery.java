package whelk.search2;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.And;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PredicateObjectQuery extends ObjectQuery {
    public PredicateObjectQuery(QueryParams queryParams, AppParams appParams, VocabMappings vocabMappings, ESSettings esSettings, Whelk whelk) throws InvalidQueryException {
        super(queryParams, appParams, vocabMappings, esSettings, whelk);
    }

    @Override
    protected Object doGetEsQueryDsl() {
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
            JsonLd ld = whelk.getJsonld();
            List<Node> altTrees = new ArrayList<>();
            if (!noDomain.isEmpty()) {
                altTrees.add(predicateObjectFilter(noDomain));
            }
            predicatesByInferredSubjectType.forEach((type, preds) ->
                altTrees.add(new And(List.of(new Type(type, ld), predicateObjectFilter(preds))))
            );
            queryTree = new QueryTree(altTrees.size() == 1 ? altTrees.getFirst() : new Or(altTrees))
                    .merge(getFullQueryTree(), ld);
        }

        if (queryParams.skipStats) {
            return getEsQueryDsl(getEsQuery(queryTree));
        }

        Set<String> subjectTypes = Stream.concat(queryTree.getRdfSubjectTypesList().stream(), predicatesByInferredSubjectType.keySet().stream())
                .collect(Collectors.toSet());
        var aggQuery = getEsAggQuery(subjectTypes);
        var postFilter = getPostFilter(subjectTypes);

        return getEsQueryDsl(getEsQuery(queryTree), aggQuery, postFilter);
    }

    private Node predicateObjectFilter(Collection<Property> predicates) {
        var preds = predicates.stream()
                .map(p -> (Node) new PathValue(p, Operator.EQUALS, object))
                .toList();
        return preds.size() == 1 ? preds.getFirst() : new Or(preds);
    }

    private List<Property> predicates() {
        return queryParams.predicates.stream().map(p -> new Property(p, whelk.getJsonld())).toList();
    }
}

package whelk.search2.querytree;

import whelk.search2.SelectedFacets;

import java.util.List;

public record PostFilter(QueryTree qt) {
    public static PostFilter extract(ExpandedQueryTree qt, SelectedFacets selectedFacets) {
        // TODO
        return new PostFilter(qt);
    }

    public List<Node> flattenedConditions() {
        return qt.allDescendants()
                .filter(Condition.class::isInstance)
                .toList();
    }

    public boolean isEmpty() {
        return qt.isEmpty();
    }

//    private static Map<String, Object> getEsMmSelectedFacets(Map<String, List<Node>> mmSelected,
//                                                             Collection<String> rdfSubjectTypes,
//                                                             JsonLd jsonLd,
//                                                             ESSettings esSettings) {
//        if (mmSelected.isEmpty()) {
//            return Map.of();
//        }
//        List<Node> orGrouped = mmSelected.values()
//                .stream()
//                .map(selected -> selected.size() > 1 ? new Or(selected) : selected.getFirst())
//                .toList();
//        return (orGrouped.size() == 1 ? orGrouped.getFirst() : new And(orGrouped))
//    }
}

package whelk.search2.querytree;

import whelk.search2.SelectedFacets;

import java.util.List;

public record PostFilter(QueryTree qt) {
    public static PostFilter extract(ExpandedQueryTree eqt, SelectedFacets selectedFacets) {
        List<List<Node>> multiSelected = selectedFacets.getAllMultiOrRadioSelected()
                .values()
                .stream()
                .map(origSelected -> origSelected.stream().map(eqt.nodeMap()::get).toList())
                .toList();

        return new PostFilter(SelectedFacets.buildMultiSelectedTree(multiSelected));
    }

    public List<Node> flattenedConditions() {
        return qt.allDescendants()
                .filter(Condition.class::isInstance)
                .toList();
    }

    public boolean isEmpty() {
        return qt.isEmpty();
    }
}

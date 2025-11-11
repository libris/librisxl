package whelk.search2;

import whelk.search2.querytree.Node;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.YearRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class SelectedFacets {
    private final Map<String, List<Node>> selectedByPropertyKey = new HashMap<>();
    private final Map<String, Query.Connective> propertyKeyToConnective = new HashMap<>();
    private final Set<String> rangeProps = new HashSet<>();

    // TODO: don't hardcode
    private final Set<String> radioProps = Set.of(
            "_categoryByCollection.find",
            "_categoryByCollection.identify"
    );


    public SelectedFacets(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        //FIXME
        for (String radioProp : radioProps) {
            selectedByPropertyKey.put(radioProp, new ArrayList<>());
        }

        init(queryTree, sliceList);
    }

    public boolean isSelectable(String propertyKey) {
        return selectedByPropertyKey.containsKey(propertyKey);
    }

    public boolean isMultiOrRadio(String propertyKey) {
        return isMultiSelectable(propertyKey) || isRadioButton(propertyKey);
    }

    public boolean isRadioButton(String propertyKey) {
        return isSelectable(propertyKey) && radioProps.contains(propertyKey);
    }

    private boolean isMultiSelectable(String propertyKey) {
        return isSelectable(propertyKey) && Query.Connective.OR.equals(propertyKeyToConnective.get(propertyKey));
    }

    public List<Node> getSelected(String propertyKey) {
        return selectedByPropertyKey.get(propertyKey);
    }

    public boolean isSelected(PathValue pathValue, String propertyKey) {
        return isSelectable(propertyKey) && selectedByPropertyKey.get(propertyKey).contains(pathValue);
    }

    public Map<String, List<Node>> getAllMultiOrRadioSelected() {
        Map<String, List<Node>> result = new HashMap<>();
        selectedByPropertyKey.forEach((pKey, selected) -> {
            if (isMultiOrRadio(pKey) && !getSelected(pKey).isEmpty()) {
                result.put(pKey, getSelected(pKey));
            }
        });
        return result;
    }

    public List<Node> getRangeSelected(String propertyKey) {
        return getSelected(propertyKey).stream()
                .filter(n -> ((PathValue) n).operator().isRange() || ((PathValue) n).value() instanceof YearRange)
                .toList();
    }

    public Query.Connective getConnective(String propertyKey) {
        return propertyKeyToConnective.getOrDefault(propertyKey, Query.Connective.AND);
    }

    public boolean isRangeFilter(String propertyKey) {
        return rangeProps.contains(propertyKey);
    }

    private void addSlice(AppParams.Slice slice, QueryTree queryTree) {
        {
            String pKey = slice.propertyKey();

            if (slice.isRange()) {
                rangeProps.add(pKey);
            }

            var property = slice.getProperty();

            Predicate<Node> isProperty = n -> n instanceof PathValue pv && pv.hasEqualProperty(property);
            Predicate<Node> hasEqualsOp = n -> ((PathValue) n).operator().equals(Operator.EQUALS);
            Predicate<Node> isPropertyEquals = n -> isProperty.test(n) && hasEqualsOp.test(n);

            List<PathValue> allNodesWithProperty = queryTree.allDescendants().filter(isProperty).map(PathValue.class::cast).toList();

            if (slice.subSlice() != null) {
                addSlice(slice.subSlice(), queryTree);
            }

            if (allNodesWithProperty.isEmpty()) {
                selectedByPropertyKey.put(pKey, List.of());
                propertyKeyToConnective.put(pKey, slice.defaultConnective());
                return;
            }

            List<Node> selected = queryTree.getTopNodes().stream()
                    .filter(slice.isRange() ? isProperty : isPropertyEquals)
                    .toList();
            
            List<Or> multiSelected = queryTree.getTopNodesOfType(Or.class).stream()
                    .filter(or -> or.children().stream().allMatch(isPropertyEquals))
                    .toList();

            if (selected.isEmpty() && multiSelected.size() == 1) {
                var children = multiSelected.getFirst().children();
                if (children.equals(allNodesWithProperty)) {
                    selectedByPropertyKey.put(pKey, children);
                    propertyKeyToConnective.put(pKey, Query.Connective.OR);
                }
            } else if (multiSelected.isEmpty() && selected.equals(allNodesWithProperty)) {
                selectedByPropertyKey.put(pKey, selected);
                propertyKeyToConnective.put(pKey, selected.size() == 1 ? slice.defaultConnective() : Query.Connective.AND);
            }
        }
    }

    private void init(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        for (var slice : sliceList) {
            addSlice(slice, queryTree);
        }
    }
}

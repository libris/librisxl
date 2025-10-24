package whelk.search2;

import whelk.search2.querytree.Node;
import whelk.search2.querytree.Numeric;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SelectedFacets {
    private final Map<String, List<Node>> selectedByPropertyKey = new HashMap<>();
    private final Map<String, Query.Connective> propertyKeyToConnective = new HashMap<>();
    private final Set<String> rangeProps = new HashSet<>();

    // TODO: don't hardcode
    private final Set<String> menuProps = Set.of(
            "_categoryByCollection.find",
            "_categoryByCollection.identify"
    );


    public SelectedFacets(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        //FIXME
        for (String menuProp : menuProps) {
            selectedByPropertyKey.put(menuProp, new ArrayList<>());
        }

        init(queryTree, sliceList);
    }

    public boolean isSelectable(String propertyKey) {
        return selectedByPropertyKey.containsKey(propertyKey);
    }

    public boolean isMultiOrMenu(String propertyKey) {
        return isMultiSelectable(propertyKey) || isMenuSelectable(propertyKey);
    }

    public boolean isMenuSelectable(String propertyKey) {
        return isSelectable(propertyKey) && menuProps.contains(propertyKey);
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

    public Map<String, List<Node>> getAllMultiOrMenuSelected() {
        Map<String, List<Node>> result = new HashMap<>();
        selectedByPropertyKey.forEach((pKey, selected) -> {
            if (isMultiOrMenu(pKey) && !getSelected(pKey).isEmpty()) {
                result.put(pKey, getSelected(pKey));
            }
        });
        return result;
    }

    public List<Node> getRangeSelected(String propertyKey) {
        return getSelected(propertyKey).stream()
                .filter(n -> ((PathValue) n).operator().isRange())
                .toList();
    }

    public Query.Connective getConnective(String propertyKey) {
        return propertyKeyToConnective.get(propertyKey);
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

            // FIXME
            //var property = slice.getProperty() instanceof Property.Ix ix
            //        ? ix.term()
            //       : slice.getProperty();

            Predicate<Node> isProperty = n -> n instanceof PathValue pv && pv.hasEqualProperty(pKey);
            Predicate<Node> hasEqualsOp = n -> ((PathValue) n).operator().equals(Operator.EQUALS);
            Predicate<Node> hasRangeOp = n -> ((PathValue) n).operator().isRange();
            Predicate<Node> hasNumericValue = n -> ((PathValue) n).value() instanceof Numeric;
            Predicate<Node> isPropertyEquals = n -> isProperty.test(n) && hasEqualsOp.test(n);

            List<PathValue> allNodesWithProperty = queryTree.allDescendants().filter(isProperty).map(PathValue.class::cast).toList();

            if (allNodesWithProperty.isEmpty()) {
                selectedByPropertyKey.put(pKey, List.of());
                propertyKeyToConnective.put(pKey, slice.defaultConnective());
                return;
            }

            List<Node> selected = queryTree.getTopNodes().stream()
                    .filter(isPropertyEquals)
                    .toList();

            if (!selected.isEmpty() && slice.getProperty() instanceof Property.Ix ix) {

                selected = selected.stream()
                        .filter(n -> true)
                        .toList();
            }

            if (slice.isRange()) {
                List<Node> rangeSelected = queryTree.getTopNodes().stream()
                        .filter(isProperty)
                        .filter(hasRangeOp)
                        .filter(hasNumericValue)
                        .toList();
                if (!rangeSelected.isEmpty()) {
                    if (rangeSelected.equals(allNodesWithProperty)) {
                        boolean isSelectableRange = rangeSelected.stream()
                                .map(PathValue.class::cast)
                                .map(PathValue::toOrEquals)
                                .collect(Collectors.groupingBy(PathValue::operator))
                                .values()
                                .stream()
                                .allMatch(group -> group.size() == 1);
                        if (isSelectableRange) {
                            selectedByPropertyKey.put(pKey, rangeSelected);
                            propertyKeyToConnective.put(pKey, slice.defaultConnective());
                        }
                    }
                    return;
                }
            }

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

        if (slice.subSlice() != null) {
            addSlice(slice.subSlice(), queryTree);
        }
    }

    private void init(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        for (var slice : sliceList) {
            addSlice(slice, queryTree);
        }
    }
}

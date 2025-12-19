package whelk.search2;

import whelk.search2.querytree.And;
import whelk.search2.querytree.Condition;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.YearRange;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class SelectedFacets {
    private final Map<String, List<Condition>> selectedByPropertyKey = new HashMap<>();
    private final Map<String, Query.Connective> propertyKeyToConnective = new HashMap<>();
    private final Set<String> rangeProps = new HashSet<>();

    // TODO: don't hardcode
    private final Set<String> radioProps = Set.of(
            Restrictions.FIND_CATEGORY,
            Restrictions.IDENTIFY_CATEGORY
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

    public List<Condition> getSelected(String propertyKey) {
        return isSelectable(propertyKey) ? selectedByPropertyKey.get(propertyKey) : List.of();
    }

    public boolean isSelected(Condition condition, String propertyKey) {
        return isSelectable(propertyKey) && selectedByPropertyKey.get(propertyKey).contains(condition);
    }

    public Map<String, List<Condition>> getAllMultiOrRadioSelected() {
        Map<String, List<Condition>> result = new HashMap<>();
        selectedByPropertyKey.forEach((pKey, selected) -> {
            if (isMultiOrRadio(pKey) && !getSelected(pKey).isEmpty()) {
                result.put(pKey, getSelected(pKey));
            }
        });
        return result;
    }

    public List<Condition> getRangeSelected(String propertyKey) {
        return getSelected(propertyKey).stream()
                .filter(c -> c.operator().isRange() || c.value() instanceof YearRange)
                .toList();
    }

    public Query.Connective getConnective(String propertyKey) {
        return propertyKeyToConnective.getOrDefault(propertyKey, Query.Connective.AND);
    }

    public boolean isRangeFilter(String propertyKey) {
        return rangeProps.contains(propertyKey);
    }

    public static QueryTree buildMultiSelectedTree(Collection<? extends List<? extends Node>> multiSelected) {
        if (multiSelected.isEmpty()) {
            return QueryTree.newEmpty();
        }
        List<Node> orGrouped = multiSelected.stream()
                .map(selected -> selected.size() > 1
                        ? new Or(selected)
                        : selected.getFirst())
                .toList();

        return new QueryTree(orGrouped.size() == 1 ? orGrouped.getFirst() : new And(orGrouped));
    }

    private void addSlice(AppParams.Slice slice, QueryTree queryTree) {
        {
            String pKey = slice.propertyKey();

            if (slice.isRange()) {
                rangeProps.add(pKey);
            }

            var property = slice.getProperty();

            Predicate<Node> isProperty = n -> n instanceof Condition c && c.selector().equals(property);
            Predicate<Node> hasEqualsOp = n -> ((Condition) n).operator().equals(Operator.EQUALS);
            Predicate<Node> isPropertyEquals = n -> isProperty.test(n) && hasEqualsOp.test(n);

            List<Condition> allNodesWithProperty = queryTree.allDescendants().filter(isProperty).map(Condition.class::cast).toList();

            if (slice.subSlice() != null) {
                addSlice(slice.subSlice(), queryTree);
            }

            if (allNodesWithProperty.isEmpty()) {
                selectedByPropertyKey.put(pKey, List.of());
                propertyKeyToConnective.put(pKey, slice.defaultConnective());
                return;
            }

            List<Condition> selected = queryTree.getTopNodes().stream()
                    .filter(slice.isRange() ? isProperty : isPropertyEquals)
                    .map(Condition.class::cast)
                    .toList();
            
            List<List<Condition>> multiSelected = queryTree.getTopNodesOfType(Or.class).stream()
                    .map(Or::children)
                    .filter(orChildren -> orChildren.stream().allMatch(isPropertyEquals))
                    .map(selectedConditions -> selectedConditions.stream().map(Condition.class::cast).toList())
                    .toList();

            if (selected.isEmpty() && multiSelected.size() == 1) {
                var selectedConditions = multiSelected.getFirst();
                if (selectedConditions.equals(allNodesWithProperty)) {
                    selectedByPropertyKey.put(pKey, selectedConditions);
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

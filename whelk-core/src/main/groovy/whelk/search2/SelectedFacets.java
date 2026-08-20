package whelk.search2;

import whelk.search2.querytree.node.And;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.value.Any;
import whelk.search2.querytree.value.YearRange;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class SelectedFacets {
    private final Map<String, List<Condition>> selectedByPropertyKey = new HashMap<>();
    private final Map<String, List<Condition>> multiSelectedByPropertyKey = new HashMap<>();
    private final Map<String, List<Condition>> radioSelectedByPropertyKey = new HashMap<>();

    private final Set<String> selectable = new HashSet<>();

    // TODO: don't hardcode
    private final Set<String> radioProps = Set.of(
            "librissearch:findCategory",
            "librissearch:identifyCategory"
    );

    public SelectedFacets(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        init(queryTree, sliceList);
    }

    public boolean isSelectable(String propertyKey) {
        return selectable.contains(propertyKey);
    }

    public boolean isInactive(String propertyKey) {
        return getSelected(propertyKey).isEmpty();
    }

    public boolean isMultiOrRadio(String propertyKey) {
        return isOrSelected(propertyKey) || anyRadioSelected();
    }

    public boolean isRadioButton(String propertyKey) {
        return radioProps.contains(propertyKey);
    }

    public List<Condition> getSelected(String propertyKey) {
        if (isAndSelected(propertyKey)) {
            return getAndSelected(propertyKey);
        }
        if (isOrSelected(propertyKey)) {
            return getOrSelected(propertyKey);
        }
        if (isRadioSelected(propertyKey)) {
            return getRadioSelected(propertyKey);
        }
        return List.of();
    }

    public Optional<Query.Connective> inferConnective(String propertyKey) {
        if (isAndSelected(propertyKey)) {
            return Optional.of(Query.Connective.AND);
        }
        if (isOrSelected(propertyKey)) {
            return Optional.of(Query.Connective.OR);
        }
        return Optional.empty();
    }

    List<Condition> getRangeSelected(String propertyKey) {
        return getSelected(propertyKey).stream()
                .filter(c -> c.operator().isRange() || c.value() instanceof YearRange)
                .toList();
    }

    public Map<String, List<Condition>> getAllMultiOrRadioSelected() {
        Map<String, List<Condition>> result = new HashMap<>();
        result.putAll(multiSelectedByPropertyKey);
        result.putAll(radioSelectedByPropertyKey);
        return result;
    }

    public void flagMultiOrRadioSelectedForPostFilter() {
       getAllMultiOrRadioSelected().values()
               .stream()
               .flatMap(List::stream)
               .forEach(Condition::flagForPostFilter);
    }

    private boolean isAndSelected(String propertyKey) {
        return selectedByPropertyKey.containsKey(propertyKey);
    }

    private boolean isOrSelected(String propertyKey) {
        return multiSelectedByPropertyKey.containsKey(propertyKey);
    }

    private boolean isRadioSelected(String propertyKey) {
        return radioSelectedByPropertyKey.containsKey(propertyKey);
    }

    private boolean anyRadioSelected() {
        return !radioSelectedByPropertyKey.isEmpty();
    }

    private List<Condition> getAndSelected(String propertyKey) {
        return selectedByPropertyKey.get(propertyKey);
    }

    private List<Condition> getOrSelected(String propertyKey) {
        return multiSelectedByPropertyKey.get(propertyKey);
    }

    private List<Condition> getRadioSelected(String propertyKey) {
        return radioSelectedByPropertyKey.get(propertyKey);
    }

    private void addSlice(AppParams.Slice slice, QueryTree queryTree) {
        if (slice.subSlice() != null) {
            addSlice(slice.subSlice(), queryTree);
        }

        Predicate<Node> isProperty = n -> n instanceof Condition c && slice.getProperty().equals(c.selector());

        List<Condition> selected = queryTree.getTopNodes().stream()
                .filter(isProperty)
                .map(Condition.class::cast)
                .toList();

        List<List<Condition>> multiSelected = queryTree.getTopNodesOfType(Or.class).stream()
                .map(Or::children)
                .filter(orChildren -> orChildren.stream().allMatch(isProperty))
                .map(selectedConditions -> selectedConditions.stream().map(Condition.class::cast).toList())
                .toList();

        String pKey = slice.getProperty().name();

        if (selected.isEmpty() && multiSelected.size() == 1 && !isRadioButton(pKey)) {
            multiSelectedByPropertyKey.put(pKey, multiSelected.getFirst());
        } else if (selected.size() == 1 && multiSelected.isEmpty()) {
            if (isRadioButton(pKey)) {
                radioSelectedByPropertyKey.put(pKey, selected);
            } else {
                switch (slice.defaultConnective()) {
                    case AND -> selectedByPropertyKey.put(pKey, selected);
                    case OR -> multiSelectedByPropertyKey.put(pKey, selected);
                }
            }
        } else if (selected.size() > 1 && multiSelected.isEmpty()) {
            selectedByPropertyKey.put(pKey, selected);
        } else if (!selected.isEmpty() || !multiSelected.isEmpty()) {
            // Can't be mirrored in facets
            return;
        }

        selectable.add(pKey);
    }

    private void init(QueryTree queryTree, List<AppParams.Slice> sliceList) {
        for (var slice : sliceList) {
            addSlice(slice, queryTree);
        }
    }
}

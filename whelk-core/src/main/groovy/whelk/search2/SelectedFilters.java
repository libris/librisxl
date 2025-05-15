package whelk.search2;

import whelk.search2.querytree.ActiveFilter;
import whelk.search2.querytree.Literal;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SelectedFilters {
    private final Map<Filter, List<Node>> activatedSiteFilters = new HashMap<>();
    private final Map<Filter, List<Node>> deactivatedSiteFilters = new HashMap<>();

    private final Map<String, List<Node>> selectedByPropertyKey = new HashMap<>();
    private final Map<String, Query.Connective> propertyKeyToConnective = new HashMap<>();
    private final Set<String> rangeProps = new HashSet<>();

    public SelectedFilters(QueryTree queryTree, AppParams appParams) {
        init(queryTree, appParams);
    }

    public SelectedFilters(QueryTree queryTree, AppParams.SiteFilters siteFilters) {
        init(queryTree, siteFilters);
    }

    public boolean isSelectable(String propertyKey) {
        return selectedByPropertyKey.containsKey(propertyKey);
    }

    public boolean isMultiSelectable(String propertyKey) {
        return isSelectable(propertyKey) && Query.Connective.OR.equals(propertyKeyToConnective.get(propertyKey));
    }

    public List<Node> getSelected(String propertyKey) {
        return selectedByPropertyKey.get(propertyKey);
    }

    public boolean isSelected(PathValue pathValue, String propertyKey) {
        return isSelectable(propertyKey) && selectedByPropertyKey.get(propertyKey).contains(pathValue);
    }

    public List<List<Node>> getAllMultiSelected() {
        return selectedByPropertyKey.keySet().stream()
                .filter(this::isMultiSelectable)
                .map(this::getSelected)
                .filter(Predicate.not(List::isEmpty))
                .toList();
    }

    public List<Node> getRangeSelected(String propertyKey) {
        return getSelected(propertyKey).stream()
                .filter(n -> Operator.rangeOperators().contains(((PathValue) n).operator()))
                .toList();
    }

    public Query.Connective getConnective(String propertyKey) {
        return propertyKeyToConnective.get(propertyKey);
    }

    public boolean isRangeFilter(String propertyKey) {
        return rangeProps.contains(propertyKey);
    }

    public boolean isActivated(Filter filter) {
        return activatedSiteFilters.containsKey(filter);
    }

    public boolean isExplicitlyDeactivated(Filter filter) {
        return deactivatedSiteFilters.containsKey(filter);
    }

    public List<Node> getActivatingNodes(Filter filter) {
        return activatedSiteFilters.get(filter);
    }

    public List<Node> getDeactivatingNodes(Filter filter) {
        return deactivatedSiteFilters.get(filter);
    }

    private void init(QueryTree queryTree, AppParams appParams) {
        for (AppParams.Slice slice : appParams.statsRepr.sliceList()) {
            String pKey = slice.propertyKey();

            if (slice.isRange()) {
                rangeProps.add(pKey);
            }

            Predicate<Node> isProperty = n -> n instanceof PathValue pv && pv.hasEqualProperty(slice.propertyKey());
            Predicate<Node> hasEqualsOp = n -> ((PathValue) n).operator().equals(Operator.EQUALS);
            Predicate<Node> hasRangeOp = n -> Operator.rangeOperators().contains(((PathValue) n).operator());
            Predicate<Node> hasNumericValue = n -> ((PathValue) n).value() instanceof Literal l && l.isNumeric();
            Predicate<Node> isPropertyEquals = n -> isProperty.test(n) && hasEqualsOp.test(n);

            List<PathValue> allNodesWithProperty = queryTree.allDescendants().filter(isProperty).map(PathValue.class::cast).toList();

            if (allNodesWithProperty.isEmpty()) {
                selectedByPropertyKey.put(pKey, List.of());
                propertyKeyToConnective.put(pKey, slice.defaultConnective());
                continue;
            }

            List<Node> selected = queryTree.getTopLevelNodes().stream()
                    .filter(isPropertyEquals)
                    .toList();

            if (slice.isRange()) {
                List<Node> rangeSelected = queryTree.getTopLevelNodes().stream()
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
                    continue;
                }
            }

            List<Or> multiSelected = queryTree.getTopLevelNodesOfType(Or.class).stream()
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

        init(queryTree, appParams.siteFilters);
    }

    private void init(QueryTree queryTree, AppParams.SiteFilters siteFilters) {
        for (AppParams.SiteFilter sf : siteFilters.getAllFilters()) {
            Filter f = sf.filter();

            if (queryTree.topLevelContains(f.getParsed())) {
                activatedSiteFilters.computeIfAbsent(f, x -> new ArrayList<>()).add(f.getParsed());
            }
            if (queryTree.topLevelContains(f.getParsed().getInverse())) {
                deactivatedSiteFilters.computeIfAbsent(f, x -> new ArrayList<>()).add(f.getParsed().getInverse());
            }

            if (f instanceof Filter.AliasedFilter af) {
                if (queryTree.topLevelContains(af.getActive())) {
                    activatedSiteFilters.computeIfAbsent(f, x -> new ArrayList<>()).add(af.getActive());
                }
                Node inverse = af.getActive().getInverse();

                if (queryTree.topLevelContains(inverse)) {
                    deactivatedSiteFilters.computeIfAbsent(f, x -> new ArrayList<>()).add(inverse);
                }

                if (inverse instanceof ActiveFilter(Filter.AliasedFilter aliasedFilter)) {
                    if (isActivated(af)) {
                        deactivatedSiteFilters.computeIfAbsent(aliasedFilter, x -> new ArrayList<>())
                                .addAll(getActivatingNodes(af));
                    }
                }
            }
        }
    }
}


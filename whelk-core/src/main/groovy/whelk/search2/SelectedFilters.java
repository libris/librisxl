package whelk.search2;

import whelk.search2.querytree.And;
import whelk.search2.querytree.Filter;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Numeric;
import whelk.search2.querytree.Or;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.QueryTree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
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

    public Map<String, List<Node>> getAllMultiSelected() {
        Map<String, List<Node>> multiSelected = new HashMap<>();
        selectedByPropertyKey.forEach((pKey, selected) -> {
            if (isMultiSelectable(pKey) && !getSelected(pKey).isEmpty()) {
                multiSelected.put(pKey, getSelected(pKey));
            }
        });
        return multiSelected;
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
        for (AppParams.Slice slice : appParams.sliceList) {
            String pKey = slice.propertyKey();

            if (slice.isRange()) {
                rangeProps.add(pKey);
            }

            // TODO this is just a workaround. Need to properly handle different levels
            if (slice.subSlice() != null) {
                String subKey = slice.subSlice().propertyKey();
                selectedByPropertyKey.put(subKey, List.of());
                propertyKeyToConnective.put(subKey, slice.subSlice().defaultConnective());
            }

            Predicate<Node> isProperty = n -> n instanceof PathValue pv && pv.hasEqualProperty(slice.propertyKey());
            Predicate<Node> hasEqualsOp = n -> ((PathValue) n).operator().equals(Operator.EQUALS);
            Predicate<Node> hasRangeOp = n -> ((PathValue) n).operator().isRange();
            Predicate<Node> hasNumericValue = n -> ((PathValue) n).value() instanceof Numeric;
            Predicate<Node> isPropertyEquals = n -> isProperty.test(n) && hasEqualsOp.test(n);

            List<PathValue> allNodesWithProperty = queryTree.allDescendants().filter(isProperty).map(PathValue.class::cast).toList();

            if (allNodesWithProperty.isEmpty()) {
                selectedByPropertyKey.put(pKey, List.of());
                propertyKeyToConnective.put(pKey, slice.defaultConnective());
                continue;
            }

            List<Node> selected = queryTree.getTopNodes().stream()
                    .filter(isPropertyEquals)
                    .toList();

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
                    continue;
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

        init(queryTree, appParams.siteFilters);
    }

    private void init(QueryTree queryTree, AppParams.SiteFilters siteFilters) {
        for (AppParams.SiteFilter sf : siteFilters.getAllFilters()) {
            Filter f = sf.filter();

            BiConsumer<Node, Map<Filter, List<Node>>> detectPresentFilter = (filterNode, map) -> {
                if (filterNode instanceof And and) {
                    List<Node> matching = queryTree.findTopNodesByCondition(and.children()::contains);
                    if (matching.size() == and.children().size()) {
                        map.computeIfAbsent(f, x -> new ArrayList<>()).addAll(matching);
                    }
                }
                Optional<Node> matching = queryTree.findTopNodeByCondition(filterNode::equals);
                matching.ifPresent(node -> map.computeIfAbsent(f, x -> new ArrayList<>()).add(node));
            };

            detectPresentFilter.accept(f.getParsed(), activatedSiteFilters);
            detectPresentFilter.accept(f.getParsed().getInverse(), deactivatedSiteFilters);

            if (f instanceof Filter.AliasedFilter af) {
                detectPresentFilter.accept(af, activatedSiteFilters);
                Node inverse = af.getInverse();
                detectPresentFilter.accept(inverse, deactivatedSiteFilters);
                if (inverse instanceof Filter.AliasedFilter aliasedFilter) {
                    if (isActivated(af)) {
                        deactivatedSiteFilters.computeIfAbsent(aliasedFilter, x -> new ArrayList<>())
                                .addAll(getActivatingNodes(af));
                    }
                }
            }
        }
    }
}


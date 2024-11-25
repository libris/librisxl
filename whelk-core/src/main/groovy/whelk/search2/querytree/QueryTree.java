package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.OutsetType;
import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;
import static whelk.search2.querytree.QueryTreeBuilder.buildTree;

public class QueryTree {
    public Node tree;
    private QueryTree filtered;
    private String freeTextPart;
    private OutsetType outsetType;

    public QueryTree(String queryString, Disambiguate disambiguate,
                     Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        if (!queryString.isEmpty()) {
            this.tree = buildTree(queryString, disambiguate, aliasToFilter);
            normalizeFreeText();
            removeNeedlessWildcard();
        }
    }

    public QueryTree(Node tree) {
        this.tree = tree;
        removeNeedlessWildcard();
    }

    public Map<String, Object> toEs(QueryUtil queryUtil, Disambiguate disambiguate) {
        return (isFiltered() ? filtered.tree : tree)
                .expand(disambiguate, getOutsetType())
                .insertNested(queryUtil::getNestedPath)
                .toEs(queryUtil.lensBoost.computeBoostFieldsFromLenses(new String[0])); // TODO: Implement boosting
    }

    public Map<String, Object> toSearchMapping(Map<String, String> nonQueryParams) {
        return isEmpty()
                ? Collections.emptyMap()
                : tree.toSearchMapping(this, nonQueryParams);
    }

    public Map<String, String> makeUpLink(Node n, Map<String, String> nonQueryParams) {
        QueryTree reducedTree = excludeFromTree(n);
        String upUrl = QueryUtil.makeFindUrl(reducedTree, nonQueryParams);
        return Map.of(JsonLd.ID_KEY, upUrl);
    }

    public OutsetType getOutsetType() {
        return outsetType == null ? OutsetType.RESOURCE : outsetType;
    }

    public void setOutsetType(Disambiguate disambiguate) {
        this.outsetType = disambiguate.decideOutset(isFiltered() ? filtered : this);
    }

    /**
     * There is no freetext or all freetext nodes are "*"
     */
    public boolean isWild() {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false)
                .noneMatch(n -> n instanceof FreeText && !((FreeText) n).isWild());
    }

    private static Iterable<Node> allDescendants(Node node) {
        Iterator<Node> i = new Iterator<>() {
            List<Node> nodes;

            @Override
            public boolean hasNext() {
                if (nodes == null) {
                    nodes = new LinkedList<>();
                    nodes.add(node);
                }
                return !nodes.isEmpty();
            }

            @Override
            public Node next() {
                Node next = nodes.removeFirst();
                nodes.addAll(next.children());
                return next;
            }
        };

        return () -> i;
    }

    public void normalizeFreeText() {
        this.tree = normalizeFreeText(tree);
    }

    private static Node normalizeFreeText(Node node) {
        return switch (node) {
            case And and -> {
                List<Node> conjuncts = new ArrayList<>();
                List<String> ftStrings = new ArrayList<>();
                for (Node n : and.children()) {
                    if (isFreeText(n)) {
                        ftStrings.add(quoteIfPhraseOrContainsSpecialSymbol(((FreeText) n).value()));
                    } else {
                        conjuncts.add(normalizeFreeText(n));
                    }
                }
                if (!ftStrings.isEmpty()) {
                    conjuncts.addFirst(new FreeText(Operator.EQUALS, String.join(" ", ftStrings)));
                }
                yield conjuncts.size() > 1 ? new And(conjuncts) : conjuncts.getFirst();
            }
            case Or or -> or.mapAndReinstantiate(QueryTree::normalizeFreeText);
            case FreeText ft -> new FreeText(ft.operator(), quoteIfPhraseOrContainsSpecialSymbol(ft.value()));
            case null, default -> node;
        };
    }

    public QueryTree replaceFreeText(String replacement) {
        if (isFreeText()) {
            return new QueryTree(new FreeText(Operator.EQUALS, replacement));
        }
        if (tree instanceof And) {
            return new QueryTree(
                    ((And) tree).mapAndReinstantiate(n -> isFreeText(n)
                            ? new FreeText(Operator.EQUALS, replacement)
                            : n)
            );
        }
        throw new RuntimeException("Failed to replace free text"); // Should never be reached
    }

    public QueryTree addToTopLevel(Node node) {
        return new QueryTree(addToTopLevel(tree, node));
    }

    private static Node addToTopLevel(Node tree, Node node) {
        return switch (tree) {
            case null -> node;
            case And and -> and.add(node);
            default -> tree.equals(node) ? tree : new And(List.of(tree, node));
        };
    }

    public QueryTree excludeFromTree(Node node) {
        return new QueryTree(excludeFromTree(node, tree));
    }

    private static Node excludeFromTree(Node nodeToExclude, Node tree) {
        if (nodeToExclude == tree) {
            return null;
        }
        return switch (tree) {
            case Group group -> group.mapFilterAndReinstantiate(c -> excludeFromTree(nodeToExclude, c), Objects::nonNull);
            default -> tree;
        };
    }

    public QueryTree removeTopLevelNode(Node node) {
        return new QueryTree(removeTopLevelNode(tree, node));
    }

    private static Node removeTopLevelNode(Node tree, Node node) {
        return switch (tree) {
            case And and -> and.remove(node);
            default -> node.equals(tree) ? null : tree;
        };
    }

    public QueryTree removeTopLevelPropValueWithRangeIfPropEquals(Property property) {
        Predicate<Node> p = (node -> node instanceof PropertyValue
                && ((PropertyValue) node).property().equals(property)
                && Operator.rangeOperators().contains(((PropertyValue) node).operator()));
        return new QueryTree(removeTopLevelNodesByCondition(tree, p));
    }

    public QueryTree removeTopLevelPropValueIfPropEquals(Property property) {
        Predicate<Node> p = (node -> node instanceof PropertyValue
                && ((PropertyValue) node).property().equals(property));
        return new QueryTree(removeTopLevelNodesByCondition(tree, p));
    }

    private static Node removeTopLevelNodesByCondition(Node tree, Predicate<Node> p) {
        // Remove all nodes meeting the condition p
        return switch (tree) {
            case null -> null;
            case And and -> and.filterAndReinstantiate(Predicate.not(p));
            default -> p.test(tree) ? null : tree;
        };
    }

    public Set<String> collectGivenTypes() {
        return collectGivenTypes(tree, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case And and -> and.children().forEach(c -> collectGivenTypes(c, types));
            case Or or -> or.children().forEach(d -> collectGivenTypes(d, types));
            case PropertyValue pv -> {
                if (pv.property().isRdfType() && pv.operator().equals(Operator.EQUALS)) {
                    types.add(pv.value().string());
                }
            }
            default -> {
                // Nothing to do here
            }
        }

        return types;
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public boolean isFreeText() {
        return isFreeText(tree);
    }

    private static boolean isFreeText(Node node) {
        return node instanceof FreeText && ((FreeText) node).operator().equals(Operator.EQUALS);
    }

    public Set<InactiveBoolFilter> getInactiveBfNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof InactiveBoolFilter)
                .map(InactiveBoolFilter.class::cast)
                .collect(Collectors.toSet());
    }

    public Set<ActiveBoolFilter> getActiveBfNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof ActiveBoolFilter)
                .map(ActiveBoolFilter.class::cast)
                .collect(Collectors.toSet());
    }

    public List<PropertyValue> getTopLevelPvNodes() {
        return getTopLevelNodes().stream()
                .filter(n -> n instanceof PropertyValue)
                .map(PropertyValue.class::cast)
                .toList();
    }

    public Set<Node> getTopLevelNodes() {
        return switch (tree) {
            case And and -> new HashSet<>(and.children());
            case null -> Collections.emptySet();
            default -> Set.of(tree);
        };
    }

    public String getTopLevelFreeText() {
        if (freeTextPart == null) {
            if (tree instanceof And) {
                freeTextPart = tree.children()
                        .stream()
                        .filter(QueryTree::isFreeText)
                        .map(n -> ((FreeText) n).value())
                        .findFirst()
                        .orElse("");
            } else if (isFreeText()) {
                freeTextPart = ((FreeText) tree).value();
            } else {
                freeTextPart = "";
            }
        }

        return freeTextPart;
    }

    private void resetFreeTextPart() {
        this.freeTextPart = null;
    }

    @Override
    public String toString() {
        return isEmpty() ? "*" : tree.toString(true);
    }

    public void removeNeedlessWildcard() {
        if (!isFreeText() && Operator.WILDCARD.equals(getTopLevelFreeText())) {
            this.tree = removeTopLevelNode(tree, new FreeText(Operator.EQUALS, Operator.WILDCARD));
            resetFreeTextPart();
        }
    }

    public QueryTree normalizeFilters(AppParams.SiteFilters siteFilters) {
        return mapToAliases(siteFilters.aliasToFilter().values())
                .removeDefaultFilters(siteFilters.getAllDefaultFilterNodes())
                .removeNegatedSelectableFilters(siteFilters.getSelectableFilterAliases());
    }

    private QueryTree mapToAliases(Collection<AppParams.Filter> aliasedFilters) {
        if (isEmpty()) {
            return this;
        }

        var newTree = tree;

        for (var filter : aliasedFilters) {
            switch (newTree) {
                case And and -> {
                    if (and.contains(filter.getExplicit())) {
                        newTree = and.replace(filter.getExplicit(), filter.getAlias().get());
                    }
                }
                default -> {
                    if (filter.getExplicit().equals(newTree)) {
                        newTree = filter.getAlias().get();
                    }
                }
            }
        }

        return new QueryTree(newTree);
    }

    private QueryTree removeDefaultFilters(Collection<Node> defaultFilters) {
        var newTree = tree;

        for (var filter : defaultFilters) {
            switch (newTree) {
                case And and -> {
                    if (and.contains(filter)) {
                        newTree = and.remove(filter);
                    }
                }
                case null -> {}
                default -> {
                    if (filter.equals(newTree)) {
                        newTree = null;
                    }
                }
            }
        }

        return new QueryTree(newTree);
    }

    private QueryTree removeNegatedSelectableFilters(Collection<String> selectableFilterAliases) {
        var newTree = tree;

        for (var ibf : getInactiveBfNodes()) {
            if (selectableFilterAliases.contains(ibf.alias())) {
                switch (newTree) {
                    case And and -> newTree = and.remove(ibf);
                    case null -> {}
                    default -> newTree = null;
                }
            }
        }

        return new QueryTree(newTree);
    }

    public void addFilters(QueryParams queryParams, AppParams appParams) {
        boolean typeNotGiven = collectGivenTypes().isEmpty();
        var currentActiveBfNodes = getActiveBfNodes();

        Function<PropertyValue, Boolean> isTypeEquals = pv ->
                pv.property().isRdfType() && pv.operator().equals(Operator.EQUALS);

        var newTree = tree;
        for (var node : getFilters(queryParams, appParams)) {
            switch (node) {
                case PropertyValue pv -> {
                    if (isTypeEquals.apply(pv)) {
                        if (typeNotGiven) {
                            newTree = addToTopLevel(newTree, pv);
                        }
                    } else {
                        newTree = addToTopLevel(newTree, pv);
                    }
                }
                case ActiveBoolFilter abf -> {
                    if (currentActiveBfNodes.stream().noneMatch(bf -> bf.nullifies(abf))) {
                        newTree = addToTopLevel(newTree, abf);
                    }
                }
                default -> newTree = addToTopLevel(newTree, node);
            }
        }

        this.filtered = new QueryTree(newTree);
    }

    private boolean isFiltered() {
        return filtered != null;
    }

    private List<Node> getFilters(QueryParams queryParams, AppParams appParams) {
        var siteFilters = appParams.siteFilters;
        var object = queryParams.object;
        var predicates = queryParams.predicates;

        var filters = new ArrayList<>(siteFilters.getDefaultFilterNodes());
        if (object == null) {
            filters.addAll(siteFilters.getDefaultTypeFilterNodes());
        }
        if (object != null) {
            if (predicates.isEmpty()) {
                filters.add(new PathValue("_links", Operator.EQUALS, object));
            } else {
                filters.addAll(predicates.stream()
                        .map(p -> new PropertyValue(p, Operator.EQUALS, new Link(object)))
                        .toList()
                );
            }
        }

        return filters;
    }
}

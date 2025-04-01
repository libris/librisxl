package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.Whelk;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.EsBoost;
import whelk.search2.Operator;

import whelk.search2.QueryParams;
import whelk.search2.QueryUtil;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;
import static whelk.search2.querytree.QueryTreeBuilder.buildTree;

public class QueryTree {
    public Node tree;
    private QueryTree filtered;
    private String freeTextPart;

    public QueryTree(String queryString, Disambiguate disambiguate, Whelk whelk, Map<String, AppParams.Filter> aliasToFilter) throws InvalidQueryException {
        if (!queryString.isEmpty()) {
            this.tree = buildTree(queryString, disambiguate, whelk, aliasToFilter);
            normalizeFreeText();
            removeNeedlessWildcard();
        }
    }

    public QueryTree(Node tree) {
        this.tree = tree;
        removeNeedlessWildcard();
    }

    public Map<String, Object> toEs(QueryUtil queryUtil, JsonLd jsonLd, Collection<String> boostFields) {
        if (boostFields.isEmpty()) {
            // TODO: Remove this when switching to new boost fields
            return getFiltered().tree.expand(jsonLd, List.of(), queryUtil.esBoost::getBoostFields).toEs(queryUtil::getNestedPath, List.of());
        }
        // TODO: Make this default
        return getFiltered().tree.expand(jsonLd, List.of()).toEs(queryUtil::getNestedPath, boostFields.isEmpty() ? EsBoost.BOOST_FIELDS : boostFields);
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

    /**
     * There is no freetext or all freetext nodes are "*"
     */
    public boolean isWild() {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false)
                .noneMatch(n -> n instanceof FreeText ft && !ft.isWild());
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
                List<FreeText> fts = new ArrayList<>();
                for (Node n : and.children()) {
                    if (isFreeText(n)) {
                        fts.add((FreeText) n);
                    } else {
                        conjuncts.add(normalizeFreeText(n));
                    }
                }
                if (!fts.isEmpty()) {
                    String joinedFts = fts.stream().map(FreeText::value)
                            .map(QueryUtil::quoteIfPhraseOrContainsSpecialSymbol)
                            .collect(Collectors.joining(" "));
                    conjuncts.addFirst(new FreeText(fts.getFirst().textQuery(), Operator.EQUALS, joinedFts));
                }
                yield conjuncts.size() > 1 ? new And(conjuncts) : conjuncts.getFirst();
            }
            case Or or -> or.mapAndReinstantiate(QueryTree::normalizeFreeText);
            case FreeText ft -> new FreeText(ft.textQuery(), ft.operator(), quoteIfPhraseOrContainsSpecialSymbol(ft.value()));
            case null, default -> node;
        };
    }

    public QueryTree replaceFreeText(String replacement) {
        if (isFreeText()) {
            return new QueryTree(new FreeText(((FreeText) tree).textQuery(), Operator.EQUALS, replacement));
        }
        if (tree instanceof And) {
            return new QueryTree(
                    ((And) tree).mapAndReinstantiate(n -> isFreeText(n)
                            ? new FreeText(((FreeText) n).textQuery(), Operator.EQUALS, replacement)
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
        if (tree instanceof Group g) {
            return g.mapFilterAndReinstantiate(c -> excludeFromTree(nodeToExclude, c), Objects::nonNull);
        }
        return tree;
    }

    public QueryTree removeTopLevelNode(Node node) {
        return new QueryTree(removeTopLevelNode(tree, node));
    }

    private static Node removeTopLevelNode(Node tree, Node node) {
        return tree instanceof And and
                ? and.remove(node)
                : (node.equals(tree) ? null : tree);
    }

    public QueryTree removeTopLevelPathValueWithRangeIfPropEquals(Property property) {
        Predicate<Node> p = (node -> node instanceof PathValue pv
                && pv.hasEqualProperty(property)
                && Operator.rangeOperators().contains(pv.operator()));
        return new QueryTree(removeTopLevelNodesByCondition(tree, p));
    }

    public QueryTree removeTopLevelPathValueIfPropEquals(Property property) {
        Predicate<Node> p = (node -> node instanceof PathValue pv && pv.hasEqualProperty(property));
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

    public List<String> collectRulingTypes(JsonLd jsonLd) {
        var tree = getFiltered().tree;
        if (tree instanceof And) {
            var reduced = tree.reduceTypes(jsonLd);
            if (reduced instanceof And) {
                return ((And) reduced).collectRulingTypes();
            }
        }
        return List.of();
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public boolean isFreeText() {
        return isFreeText(tree);
    }

    private static boolean isFreeText(Node node) {
        return node instanceof FreeText ft && ft.operator().equals(Operator.EQUALS);
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

    public List<PathValue> getTopLevelPvNodes() {
        return getTopLevelNodes().stream()
                .filter(PathValue.class::isInstance)
                .map(PathValue.class::cast)
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

    public String toQueryString() {
        return isEmpty() ? "*" : tree.toQueryString(true);
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    public void removeNeedlessWildcard() {
        if (!isFreeText() && Operator.WILDCARD.equals(getTopLevelFreeText())) {
            getTopLevelNodes().stream().filter(FreeText.class::isInstance)
                    .map(FreeText.class::cast)
                    .filter(FreeText::isWild)
                    .findFirst()
                    .ifPresent(ft -> {
                                this.tree = removeTopLevelNode(tree, ft);
                                resetFreeTextPart();
                    });
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
            if (newTree instanceof And and && and.contains(filter.getExplicit())) {
                newTree = and.replace(filter.getExplicit(), filter.getAlias().get());
            } else if (filter.getExplicit().equals(newTree)) {
                newTree = filter.getAlias().get();
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

    public void addFilters(QueryParams queryParams, AppParams appParams, JsonLd jsonLd) {
        var currentActiveBfNodes = getActiveBfNodes();

        var newTree = tree;
        for (var node : getFilters(queryParams, appParams, jsonLd)) {
            // Don't add type filter when type is already given somewhere in the query
            if (node.isTypeNode() && containsTypeNode(tree)) {
                continue;
            }
            // Don't add filter X if there is already something saying NOT X
            if (node instanceof ActiveBoolFilter && currentActiveBfNodes.stream().anyMatch(bf -> bf.nullifies((ActiveBoolFilter) node))) {
                continue;
            }
            newTree = addToTopLevel(newTree, node);
        }

        this.filtered = new QueryTree(newTree);
    }

    private static boolean containsTypeNode(Node tree) {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false)
                .anyMatch(Node::isTypeNode);
    }

    private QueryTree getFiltered() {
        return filtered != null ? filtered : this;
    }

    private List<Node> getFilters(QueryParams queryParams, AppParams appParams, JsonLd jsonLd) {
        var siteFilters = appParams.siteFilters;
        var object = queryParams.object;
        var predicates = queryParams.predicates;

        var filters = new ArrayList<>(siteFilters.getDefaultFilterNodes());
        if (object == null) {
            filters.addAll(siteFilters.getDefaultTypeFilterNodes());
        }
        if (object != null) {
            if (predicates.isEmpty()) {
                filters.add(new PathValue("_links", Operator.EQUALS, new Literal(object)));
            } else {
                filters.addAll(predicates.stream()
                        .map(p -> new PathValue(new Property(p, jsonLd), Operator.EQUALS, new Link(object)))
                        .toList()
                );
            }
        }

        return filters;
    }
}

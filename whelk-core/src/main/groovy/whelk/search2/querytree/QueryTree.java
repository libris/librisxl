package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.AppParams;
import whelk.search2.Disambiguate;
import whelk.search2.EsMappings;
import whelk.search2.Filter;
import whelk.search2.EsBoost;
import whelk.search2.Operator;

import whelk.search2.Query;
import whelk.search2.QueryParams;
import whelk.search2.SelectedFilters;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static whelk.search2.querytree.QueryTreeBuilder.buildTree;

public class QueryTree {
    private QueryTree filtered;

    private Node tree;

    public QueryTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        if (queryString != null && !queryString.isEmpty()) {
            this.tree = buildTree(queryString, disambiguate);
            normalizeTree();
        }
    }

    public QueryTree(Node tree) {
        this.tree = tree;
    }

    public QueryTree copy() {
        return new QueryTree(tree, filtered);
    }

    public static QueryTree empty() {
        return new QueryTree(null);
    }

    public Map<String, Object> toEs(JsonLd jsonLd,
                                    EsMappings esMappings,
                                    EsBoost.Config boostConfig,
                                    Collection<String> rulingTypes,
                                    List<Node> exclude)
    {
        return getFiltered().omitNodes(exclude)
                .expand(jsonLd, rulingTypes)
                .toEs(esMappings, boostConfig);
    }

    private QueryTree(Node tree, QueryTree filtered) {
        this.tree = tree;
        if (filtered != null) {
            this.filtered = new QueryTree(filtered.tree);
        }
    }

    public Map<String, Object> toSearchMapping(QueryParams queryParams) {
        return isEmpty()
                ? Collections.emptyMap()
                : tree.toSearchMapping(this, queryParams);
    }

    public void applySiteFilters(Query.SearchMode searchMode, AppParams.SiteFilters siteFilters, SelectedFilters selectedFilters) {
        _applySiteFilters(searchMode, siteFilters, selectedFilters);
    }

    public void applyObjectFilter(String object) {
        _applyObjectFilter(object);
    }

    public void applyPredicateObjectFilter(Collection<Property> predicates, String object) {
        _applyPredicateObjectFilter(predicates, object);
    }

    public QueryTree omitNode(Node node) {
        return omitNodes(List.of(node));
    }

    public QueryTree omitNodes(List<Node> nodes) {
        QueryTree copy = copy();
        nodes.forEach(copy::_omitNode);
        return copy;
    }

    public QueryTree addTopLevelNode(Node node) {
        QueryTree copy = copy();
        copy._addTopLevelNode(node);
        copy.normalizeTree();
        return copy;
    }

    public QueryTree removeTopLevelNode(Node node) {
        return removeTopLevelNodes(List.of(node));
    }

    public QueryTree removeTopLevelNodes(List<Node> nodes) {
        QueryTree copy = copy();
        nodes.forEach(copy::_removeTopLevelNode);
        return copy;
    }

    public QueryTree replaceTopLevelNode(Node node, Node replacement) {
        QueryTree copy = copy();
        copy._replaceTopLevelNode(node, replacement);
        copy.normalizeTree();
        return copy;
    }

    public boolean topLevelContains(Node node) {
        return topLevelContains(tree, node);
    }

    public QueryTree replaceTopLevelFreeText(String replacement) {
        QueryTree copy = copy();
        copy._replaceTopLevelFreeText(replacement);
        return copy;
    }

    public QueryTree removeTopLevelNodesByCondition(Predicate<Node> condition) {
        QueryTree copy = copy();
        copy._removeTopLevelNodesByCondition(condition);
        return copy;
    }

    public boolean isEmpty() {
        return tree == null;
    }

    public Stream<Node> allDescendants() {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false);
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

    public List<Link> collectLinks() {
        return allDescendants()
                .map(n -> n instanceof PathValue pv && pv.value() instanceof Link l ? l : null)
                .filter(Objects::nonNull)
                .toList();
    }

    public Optional<Node> findTopLevelNodeByCondition(Predicate<Node> condition) {
        return getTopLevelNodes().stream().filter(condition).findFirst();
    }

    public <T> List<T> getTopLevelNodesOfType(Class<T> nodeType) {
        return getTopLevelNodes().stream().filter(nodeType::isInstance).map(nodeType::cast).toList();
    }

    public List<Node> getTopLevelNodes() {
        return switch (tree) {
            case And and -> and.children();
            case null -> List.of();
            default -> List.of(tree);
        };
    }

    public String getFreeTextPart() {
        return findTopLevelNode(Node::isFreeTextNode)
                .map(FreeText.class::cast)
                .map(FreeText::toString)
                .orElse("");
    }

    public String toQueryString() {
        return isEmpty() ? Operator.WILDCARD : tree.toQueryString(true);
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    public QueryTree getFiltered() {
        return filtered != null ? filtered : copy();
    }

    private Node expand(JsonLd jsonLd, Collection<String> rulingTypes) {
        return tree.expand(jsonLd, rulingTypes);
    }

    private void normalizeTree() {
        removeFreeTextWildcard();
    }

    private void _omitNode(Node omit) {
        this.tree = _omitNode(tree, omit);
    }

    private void _replaceTopLevelNode(Node replace, Node replacement) {
        this.tree = _replaceTopLevelNode(tree, replace, replacement);
        normalizeTree();
    }

    private void _removeTopLevelNode(Node remove) {
        this.tree = _removeTopLevelNode(tree, remove);
    }

    private void _addTopLevelNode(Node add) {
        this.tree = _addTopLevelNode(tree, add);
        normalizeTree();
    }

    private void _replaceTopLevelFreeText(String replacement) {
        this.tree = _replaceTopLevelFreeText(tree, replacement);
    }

    private void _removeTopLevelNodesByCondition(Predicate<Node> p) {
        this.tree = _removeTopLevelNodesByCondition(tree, p);
    }

    private Optional<Node> findTopLevelNode(Predicate<Node> condition) {
        return !isEmpty() && condition.test(tree)
                ? Optional.of(tree)
                : (tree instanceof And and ? and.findChild(condition) : Optional.empty());
    }

    private void removeFreeTextWildcard() {
        if (tree != null && !isWild(tree)) {
            _removeTopLevelNodesByCondition(QueryTree::isWild);
        }
    }

    private static boolean topLevelContains(Node tree, Node node) {
        return tree instanceof And and ? and.contains(node) : node.equals(tree);
    }

    private static Node _replaceTopLevelNode(Node tree, Node replace, Node replacement) {
        return tree instanceof And and
                ? and.replace(replace, replacement)
                : (replace.equals(tree) ? replacement : tree);
    }

    private static Node _removeTopLevelNode(Node tree, Node remove) {
        return tree instanceof And and
                ? and.remove(remove)
                : (remove.equals(tree) ? null : tree);
    }

    private static Node _addTopLevelNode(Node tree, Node add) {
        return switch (tree) {
            case null -> add;
            case And and -> and.add(add);
            default -> tree.equals(add) ? tree : new And(List.of(tree, add));
        };
    }

    private static Node _omitNode(Node tree, Node omit) {
        if (omit == tree) {
            return null;
        }
        if (tree instanceof Group g) {
            return g.mapFilterAndReinstantiate(c -> _omitNode(c, omit), Objects::nonNull);
        }
        return tree;
    }

    private Node _replaceTopLevelFreeText(Node tree, String replacement) {
        return findTopLevelNode(Node::isFreeTextNode)
                .map(FreeText.class::cast)
                .map(ft -> _replaceTopLevelNode(tree, ft, ft.replace(replacement)))
                .orElse(tree);
    }

    private static Node _removeTopLevelNodesByCondition(Node tree, Predicate<Node> p) {
        // Remove all nodes meeting the condition p
        return switch (tree) {
            case null -> null;
            case And and -> and.filterAndReinstantiate(Predicate.not(p));
            default -> p.test(tree) ? null : tree;
        };
    }

    private static boolean isWild(Node node) {
        return node instanceof FreeText ft && ft.isWild();
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

        return () -> node != null ? i : Collections.emptyIterator();
    }

    private void _applySiteFilters(Query.SearchMode searchMode, AppParams.SiteFilters siteFilters, SelectedFilters selectedFilters) {
        QueryTree filtered = getFiltered();

        Predicate<AppParams.DefaultSiteFilter> isApplicable = df ->
                df.appliesTo().contains(searchMode)
                        && !selectedFilters.isActivated(df.filter())
                        // Override default filter if the original query contains its inverse.
                        // e.g. don't add "\"rdf:type\":Work" if query is "NOT \"rdf:type\":Work something"
                        && !selectedFilters.isExplicitlyDeactivated(df.filter())
                        // Override default type filter if the original query already states which types to search.
                        // e.g. don't add "\"rdf:type\":Work" if query is "\"rdf:type\":Agent Astrid Lindgren"
                        && !(df.filter().isTypeFilter() && allDescendants().anyMatch(Node::isTypeNode));

        siteFilters.defaultFilters().stream()
                .filter(isApplicable)
                .map(AppParams.DefaultSiteFilter::filter)
                .map(Filter::getParsed)
                .forEach(filtered::_addTopLevelNode);

        this.filtered = filtered;
    }

    private void _applyObjectFilter(String object) {
        QueryTree filtered = getFiltered();
        filtered._addTopLevelNode(new PathValue("_links", Operator.EQUALS, new Literal(object)));
        this.filtered = filtered;
    }

    private void _applyPredicateObjectFilter(Collection<Property> predicates, String object) {
        QueryTree filtered = getFiltered();
        predicates.stream()
                .map(p -> new PathValue(p, Operator.EQUALS, new Link(object)))
                .forEach(filtered::_addTopLevelNode);
        this.filtered = filtered;
    }
}

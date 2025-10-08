package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;

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

    public Map<String, Object> toEs(JsonLd jsonLd, ESSettings esSettings, Collection<String> rulingTypes, List<Node> exclude) {
        return getFiltered().remove(exclude)
                .expand(jsonLd, rulingTypes)
                .toEs(esSettings);
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

    public QueryTree remove(Node node) {
        return remove(List.of(node));
    }

    public QueryTree remove(List<Node> nodes) {
        QueryTree copy = copy();
        copy._remove(nodes);
        return copy;
    }

    public QueryTree replace(Node node, Node replacement) {
        QueryTree copy = copy();
        copy._replace(node, replacement);
        return copy;
    }

    public QueryTree add(Node node) {
        QueryTree copy = copy();
        copy._add(node);
        copy.normalizeTree();
        return copy;
    }

    public QueryTree replaceSimpleFreeText(String replacement) {
        return findSimpleFreeText()
                .map(ft -> replace(ft, new FreeText(replacement)))
                .orElse(this);
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

    public Optional<FreeText> findSimpleFreeText() {
        return findTopNodeByCondition(node -> node instanceof FreeText ft
                && !ft.negate()
                && ft.connective() == Query.Connective.AND).map(FreeText.class::cast);
    }

    public List<Node> findTopNodesByCondition(Predicate<Node> condition) {
        return getTopNodes().stream().filter(condition).toList();
    }

    public Optional<Node> findTopNodeByCondition(Predicate<Node> condition) {
        return getTopNodes().stream().filter(condition).findFirst();
    }

    public <T> List<T> getTopNodesOfType(Class<T> nodeType) {
        return getTopNodes().stream().filter(nodeType::isInstance).map(nodeType::cast).toList();
    }

    public List<Node> getTopNodes() {
        return switch (tree) {
            case And and -> and.children();
            case null -> List.of();
            default -> List.of(tree);
        };
    }

    public boolean isSimpleFreeText() {
        return findSimpleFreeText().map(tree::equals).orElse(false);
    }

    public String getFreeTextPart() {
        return findSimpleFreeText().map(FreeText::queryForm).orElse("");
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

    private void _remove(List<Node> remove) {
        this.tree = _remove(tree, remove);
    }

    private void _replace(Node replace, Node replacement) {
        this.tree = _replace(tree, replace, replacement);
    }

    private void _add(Node add) {
        this.tree = _add(tree, add);
        normalizeTree();
    }

    private void _removeTopNodesByCondition(Predicate<Node> p) {
        this.tree = _removeTopNodesByCondition(tree, p);
    }

    private void removeFreeTextWildcard() {
        if (tree != null && !isWild(tree)) {
            _removeTopNodesByCondition(QueryTree::isWild);
        }
    }

    private static Node _remove(Node tree, List<Node> remove) {
        if (remove.stream().anyMatch(n -> n == tree)) {
            return null;
        }
        if (tree instanceof Group g) {
            return g.mapFilterAndReinstantiate(c -> _remove(c, remove), Objects::nonNull);
        }
        return tree;
    }

    private static Node _replace(Node tree, Node replace, Node replacement) {
        if (tree == replace) {
            return replacement;
        }
        if (tree instanceof Group g) {
            return g.mapAndReinstantiate(c -> _replace(c, replace, replacement));
        }
        return tree;
    }

    private static Node _add(Node tree, Node add) {
        return switch (tree) {
            case null -> add;
            case And and -> new And(Stream.concat(and.children().stream(), Stream.of(add)).distinct().toList());
            default -> tree.equals(add) ? tree : new And(List.of(tree, add));
        };
    }

    private static Node _removeTopNodesByCondition(Node tree, Predicate<Node> p) {
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
                .forEach(filtered::_add);

        this.filtered = filtered;
    }

    private void _applyObjectFilter(String object) {
        QueryTree filtered = getFiltered();
        filtered._add(new PathValue("_links", Operator.EQUALS, new FreeText(object)));
        this.filtered = filtered;
    }

    private void _applyPredicateObjectFilter(Collection<Property> predicates, String object) {
        QueryTree filtered = getFiltered();
        predicates.stream()
                .map(p -> new PathValue(p, Operator.EQUALS, new Link(object)))
                .forEach(filtered::_add);
        this.filtered = filtered;
    }
}

package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static whelk.search2.QueryUtil.makeViewFindUrl;
import static whelk.search2.querytree.QueryTreeBuilder.buildTree;

public class QueryTree {
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

    public Node tree() {
        return tree;
    }

    public static QueryTree newEmpty() {
        return new QueryTree(null);
    }

    public ReducedQueryTree reduce(JsonLd jsonLd) {
        return isEmpty() ? ReducedQueryTree.newEmpty() : new ReducedQueryTree(tree.reduce(jsonLd));
    }

    public ExpandedQueryTree expand(JsonLd jsonLd) {
        return isEmpty() ? ExpandedQueryTree.newEmpty() : new ExpandedQueryTree(tree.expand(jsonLd, List.of()));
    }

    public ReducedQueryTree merge(QueryTree other, JsonLd jsonLd) {
        Node mergedTree = isEmpty() ? other.tree() : (other.isEmpty() ? tree : merge(tree, other.tree(), jsonLd));
        return mergedTree != null ? new ReducedQueryTree(mergedTree.reduce(jsonLd)) : ReducedQueryTree.newEmpty();
    }

    public List<String> getRdfSubjectTypesList() {
        return getRdfSubjectType().asList().stream().map(Type::type).toList();
    }

    public RdfSubjectType getRdfSubjectType() {
        return isEmpty() ? RdfSubjectType.noType() : tree.rdfSubjectType();
    }

    public Map<String, Object> toSearchMapping(QueryParams queryParams, String apiParam) {
        if (isEmpty()) {
            return Collections.emptyMap();
        }
        return tree.toSearchMapping(n -> Map.of(JsonLd.ID_KEY, makeViewFindUrl(remove(n).toQueryString(), queryParams, apiParam)));
    }

    public QueryTree remove(Node node) {
        return removeAll(List.of(node));
    }

    public QueryTree removeAll(Collection<? extends Node> nodes) {
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
        return allDescendants(tree);
    }

    public static Stream<Node> allDescendants(Node tree) {
        return StreamSupport.stream(_allDescendants(tree).spliterator(), false);
    }

    public List<Link> collectLinks() {
        return allDescendants()
                .flatMap(n -> n instanceof FilterAlias fa ? allDescendants(fa.getParsed()) : Stream.of(n))
                .map(n -> n instanceof Condition c && c.value() instanceof Link l ? l : null)
                .filter(Objects::nonNull)
                .toList();
    }

    public Optional<FreeText> findSimpleFreeText() {
        return findTopNodeByCondition(node -> node instanceof FreeText ft && ft.connective() == Query.Connective.AND)
                .map(FreeText.class::cast);
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

    public boolean implies(Node node, JsonLd jsonLd) {
        return tree.implies(node, jsonLd);
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

    protected QueryTree copy() {
        return new QueryTree(tree);
    }

    private void normalizeTree() {
        removeFreeTextWildcard();
    }

    private void _remove(Collection<? extends Node> remove) {
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

    private static Node _remove(Node tree, Collection<? extends Node> remove) {
        if (remove.stream().anyMatch(n -> n == tree)) {
            return null;
        }
        if (tree instanceof Group g) {
            return g.mapFilterAndReinstantiate(c -> _remove(c, remove), Objects::nonNull);
        }
        if (tree instanceof Not(Node node)) {
            var removed = _remove(node, remove);
            return removed != null ? new Not(removed) : null;
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

    private static Iterable<Node> _allDescendants(Node node) {
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

    private static Node merge(Node a, Node b, JsonLd jsonLd) {
        if (a instanceof Or or) {
            return or.mapAndReinstantiate(n -> merge(n, b, jsonLd));
        }

        RdfSubjectType aRdfSubjectType = a.rdfSubjectType();

        if (aRdfSubjectType.isNoType()) {
            // No type conflict, just merge as is
            return doMerge(a, b, jsonLd);
        }

        if (aRdfSubjectType.isMultiType()) {
            // type:(T1 OR T2) X --> (type:T1 X) OR (type:T2 X)
            var groupedByType = groupByType(a, aRdfSubjectType);
            var merged = merge(groupedByType, b, jsonLd);
            // If nothing was merged return the original more compact form,
            return merged.equals(groupedByType) ? a : merged;
        }

        return doMerge(a, compatibleSubTree(aRdfSubjectType.singleType().type(), b, jsonLd), jsonLd);
    }

    private static Node compatibleSubTree(String aSubjectType, Node bTree, JsonLd jsonLd) {
        if (bTree == null) {
            return null;
        }

        if (bTree instanceof Or or) {
            return or.mapFilterAndReinstantiate(n -> compatibleSubTree(aSubjectType, n, jsonLd), Objects::nonNull);
        }

        RdfSubjectType bRdfSubjectType = bTree.rdfSubjectType();

        if (bRdfSubjectType.isNoType()) {
            return compatibleByDomain(aSubjectType, bTree, jsonLd);
        }

        if (bRdfSubjectType.isMultiType()) {
            return compatibleSubTree(aSubjectType, groupByTypeAndCompatible(bTree, bRdfSubjectType, jsonLd), jsonLd);
        }

        Type bSubjectType = bRdfSubjectType.singleType();

        if (jsonLd.isSubClassOf(aSubjectType, bSubjectType.type())) {
            // Explicit b type given and compatible with a type -> assume that the entire b expression is compatible with a.
            return noTypeTree(bTree, bRdfSubjectType.asNode());
        }

        Optional<Property> aToBIntegralRelation = QueryUtil.getIntegralRelationsForType(aSubjectType, jsonLd)
                .stream()
                .filter(p -> p.range().stream()
                        .anyMatch(r -> jsonLd.isSubClassOf(bSubjectType.type(), r)))
                .findFirst();
        if (aToBIntegralRelation.isPresent()) {
            // Also compatible types, indirectly via integral relation
            Property relation = aToBIntegralRelation.get();
            Condition integralType = bSubjectType.withSelector(new Path(List.of(relation, bSubjectType.rdfTypeProperty())));
            return _replace(bTree, bSubjectType, integralType);
        }

        // b type incompatible with a type
        return null;
    }

    private static Or groupByType(Node n, RdfSubjectType nRdfSubjectType) {
        Node noTypeTree = noTypeTree(n, nRdfSubjectType.asNode());
        return new Or(nRdfSubjectType.asList().stream().map(t -> new And(List.of(t, noTypeTree))).toList());
    }

    // FIXME: Naming
    private static Or groupByTypeAndCompatible(Node n, RdfSubjectType nRdfSubjectType, JsonLd jsonLd) {
        List<Node> grouped = new ArrayList<>();
        Node noTypeTree = noTypeTree(n, nRdfSubjectType.asNode());
        for (Type t : nRdfSubjectType.asList()) {
            var compatibleInGroup = compatibleByDomain(t.type(), noTypeTree, jsonLd);
            grouped.add(compatibleInGroup == null ? t : new And(List.of(t, compatibleInGroup)));
        }
        return new Or(grouped);
    }

    private static Node noTypeTree(Node tree, Node typeNode) {
        return _remove(tree, List.of(typeNode));
    }

    private static Node compatibleByDomain(String rdfSubjectType, Node tree, JsonLd jsonLd) {
        Predicate<Condition> isCompatibleByDomain = c -> c.selector().appearsOnlyOnRecord(jsonLd)
                || c.selector().appearsOnType(rdfSubjectType, jsonLd)
                || c.selector().indirectlyAppearsOnType(rdfSubjectType, jsonLd);

        Predicate<Node> isIncompatible = node -> switch (node) {
            case Condition c -> !isCompatibleByDomain.test(c);
            case Not(Condition c) -> !isCompatibleByDomain.test(c);
            case FilterAlias ignored -> false; // TODO?
            default -> false;
        };

        List<Node> incompatibleNodes = allDescendants(tree).filter(isIncompatible).toList();

        return _remove(tree, incompatibleNodes);
    }

    private static Node doMerge(Node a, Node b, JsonLd jsonLd) {
        List<Node> nodesToKeep = new ArrayList<>(a instanceof And and ? and.children() : List.of(a));

        (b instanceof And and ? and.children().stream() : Stream.ofNullable(b))
                .filter(n -> !a.implies(n.getInverse(), jsonLd))
                .forEach(nodesToKeep::add);

        return (nodesToKeep.size() == 1 ? nodesToKeep.getFirst() : new And(nodesToKeep));
    }
}

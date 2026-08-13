package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;
import whelk.search2.*;
import whelk.search2.esquery.EsQueryTreeBuilder;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.value.Any;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.node.FilterAlias;
import whelk.search2.querytree.value.FreeText;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.value.Link;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.search2.querytree.QueryTreeBuilder.buildTree;

public class QueryTree {
    private Node tree;

    public QueryTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        this.tree = buildTree(queryString, disambiguate);
    }

    public QueryTree(Node tree) {
        this.tree = tree == null ? any() : tree;
    }

    public Node tree() {
        return tree;
    }

    public Map<String, Object> toEsQuery(ESSettings esSettings) {
        return EsQueryTreeBuilder.buildFrom(tree, esSettings).dsl();
    }

    public static QueryTree newEmpty() {
        return new QueryTree(any());
    }

    public ReducedTree reduce(JsonLd jsonLd) {
        return new ReducedTree(QueryTreeReducer.reduce(tree, jsonLd));
    }

    public ExpandedTree expand(JsonLd jsonLd) {
        return expand(jsonLd, List.of());
    }

    public ExpandedTree expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return new ExpandedTree(QueryTreeExpander.expand(tree, jsonLd, rdfSubjectTypes));
    }

    public MergedTree merge(QueryTree other, JsonLd jsonLd) {
        return new MergedTree(QueryTreeMerger.mergeAndReduce(tree, other.tree(), jsonLd));
    }

    public RdfSubjectType getRdfSubjectType() {
        return RdfSubjectType.extractFrom(tree);
    }

    public Map<String, Object> toSearchMapping(QueryParams queryParams, String apiParam) {
        return SearchMapping.buildFrom(this, queryParams, apiParam);
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
        return copy;
    }

    public QueryTree replaceSimpleFreeText(String replacement) {
        return findSimpleFreeTextQueryNode()
                .map(c -> replace(c, c.withValue(new FreeText(replacement))))
                .orElse(this);
    }

    public boolean isAny() {
        return tree instanceof Condition c && c.isAnyQuery();
    }

    public Stream<Node> allDescendants() {
        return Node.allDescendants(tree);
    }

    public List<Link> collectLinks() {
        return allDescendants()
                .flatMap(n -> n instanceof FilterAlias fa
                        ? Node.allDescendants(fa.getParsed())
                        : Stream.of(n))
                .map(n -> n instanceof Condition c && c.value() instanceof Link l ? l : null)
                .filter(Objects::nonNull)
                .toList();
    }

    public Optional<Condition> findSimpleFreeTextQueryNode() {
        return findTopNodeByCondition(node -> node instanceof Condition c
                && c.isTextQuery()
                && c.freeTextValue().connective() == Query.Connective.AND)
                .map(Condition.class::cast);
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

    public String getFreeTextPart() {
        return findSimpleFreeTextQueryNode()
                .map(Condition::freeTextValue)
                .map(FreeText::queryForm)
                .orElse("");
    }

    public String toQueryString() {
        return tree.toQueryString();
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    protected QueryTree copy() {
        return new QueryTree(tree);
    }

    private void _remove(Collection<? extends Node> remove) {
        var modified =  Node.remove(tree, remove);
        this.tree = modified == null ? any() : modified;
    }

    private void _replace(Node replace, Node replacement) {
        this.tree = Node.replace(tree, replace, replacement);
    }

    private void _add(Node add) {
        this.tree = Node.add(tree, add);
    }

    private static Condition any() {
        return new Any.EmptyString().asNode();
    }

    public static class ReducedTree extends QueryTree {
        public ReducedTree(Node tree) {
            super(tree);
        }

        @Override
        public ReducedTree reduce(JsonLd jsonLd) {
            return this;
        }

        @Override
        public ReducedTree copy() {
            return new ReducedTree(tree());
        }
    }

    public static class ExpandedTree extends QueryTree {
        ExpandedTree(Node tree) {
            super(tree);
        }

        @Override
        public ExpandedTree expand(JsonLd jsonLd) {
            return this;
        }

        @Override
        public ExpandedTree copy() {
            return new ExpandedTree(tree());
        }

        public static final class DerivedOr extends Or {
            private final Condition originalCondition;

            public DerivedOr(List<? extends Node> children, Condition originalCondition) {
                super(children, false);
                this.originalCondition = originalCondition;
            }

            public Condition originalCondition() {
                return originalCondition;
            }

            @Override
            public DerivedOr newInstance(List<Node> children, boolean flattenChildren) {
                return new DerivedOr(children, originalCondition);
            }
        }
    }

    public static class MergedTree extends QueryTree {
        public MergedTree(Node tree) {
            super(tree);
        }

        @Override
        public MergedTree copy() {
            return new MergedTree(tree());
        }
    }
}

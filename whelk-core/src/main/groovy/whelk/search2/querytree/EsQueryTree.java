package whelk.search2.querytree;

import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.SelectedFacets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.nestedWrap;

public class EsQueryTree {
    private final ESSettings esSettings;
    private final NestedTree nestedTree;
    private final PostFilterTree postFilterTree;

    public EsQueryTree(ExpandedQueryTree queryTree, ESSettings esSettings) {
        this(queryTree, esSettings, null);
    }

    public EsQueryTree(ExpandedQueryTree queryTree, ESSettings esSettings, SelectedFacets selectedFacets) {
        this.esSettings = esSettings;
        this.nestedTree = NestedTree.from(queryTree, esSettings.mappings());
        this.postFilterTree = PostFilterTree.from(queryTree, nestedTree, selectedFacets);
    }

    public Map<String, Object> getMainQuery() {
        return nestedTree.remove(postFilterTree).tree().toEs(esSettings);
    }

    public Map<String, Object> getPostFilter() {
        return postFilterTree.isEmpty() ? Map.of() : postFilterTree.tree().toEs(esSettings);
    }

    @Override
    public String toString() {
        return nestedTree.tree().toString();
    }

    private record NestedTree(Node tree) {
        private static NestedTree from(ExpandedQueryTree queryTree, EsMappings esMappings) {
            return new NestedTree(getNestedTree(queryTree.tree(), esMappings));
        }

        private static Node getNestedTree(Node tree, EsMappings esMappings) {
            if (tree instanceof And and) {
                tree = regroupAltSelectorsByNestedStem(and, esMappings);
            }
            return _getNestedTree(tree, esMappings);
        }

        private static Node _getNestedTree(Node tree, EsMappings esMappings) {
            if (tree instanceof And and) {
                List<Nested> nestedGroups = getNestedGroups(and, esMappings);
                List<Node> nested = nestedGroups.stream().map(And::children).flatMap(List::stream).toList();
                List<Node> nonNested = and.children().stream()
                        .filter(Predicate.not(nested::contains))
                        .map(n -> getNestedTree(n, esMappings)).toList();
                if (nestedGroups.size() == 1 && nonNested.isEmpty()) {
                    return nestedGroups.getFirst();
                }
                return new And(Stream.concat(nestedGroups.stream(), nonNested.stream()).toList(), false);
            }
            if (tree instanceof Or or) {
                return or.mapAndReinstantiate(n -> getNestedTree(n, esMappings));
            }
            return tree;
        }

        private static List<Nested> getNestedGroups(And and, EsMappings esMappings) {
            List<Nested> nestedGroups = new ArrayList<>();

            and.children().stream().collect(Collectors.groupingBy(node -> getNestedStem(node, esMappings)))
                    .forEach((nestedStem, group) -> {
                        // At least two different paths sharing the same nested stem
                        if (nestedStem.isPresent() && group.size() > 1) {
                            nestedGroups.add(new Nested(group, nestedStem.get()));
                        }
                    });

            return nestedGroups;
        }

        private static Group regroupAltSelectorsByNestedStem(And and, EsMappings esMappings) {
            List<Node> newChildren = new ArrayList<>(and.children());

            List<List<Or.AltSelectors>> altSelectorsGroupedByOrigSelector = and.children().stream()
                    .filter(Or.AltSelectors.class::isInstance)
                    .map(Or.AltSelectors.class::cast)
                    .collect(Collectors.groupingBy(Or.AltSelectors::origSelector))
                    .values()
                    .stream()
                    .filter(altSelectors -> altSelectors.size() > 1)
                    .toList();

            for (List<Or.AltSelectors> altSelectors : altSelectorsGroupedByOrigSelector) {
                Map<Optional<String>, List<Node>> groupedByNestedStem = altSelectors.stream()
                        .map(Node::children)
                        .flatMap(List::stream)
                        .collect(Collectors.groupingBy(n -> getNestedStem(n, esMappings)));
                if (!groupedByNestedStem.containsKey(Optional.empty())) {
                    newChildren.removeAll(altSelectors);
                    newChildren.add(new Or(groupedByNestedStem.values().stream().map(And::new).toList()));
                }
            }

            return newChildren.size() == 1 ? (Or) newChildren.getFirst() : new And(newChildren);
        }

        private static Optional<String> getNestedStem(Node node, EsMappings esMappings) {
            if (node instanceof Condition c) {
                return c.selector().getEsNestedStem(esMappings);
            }
            if (node instanceof Group g) {
                var groupedByNestedStem = g.children().stream().collect(Collectors.groupingBy(n -> getNestedStem(n, esMappings)));
                if (groupedByNestedStem.size() == 1) {
                    return groupedByNestedStem.keySet().iterator().next();
                }
            }
            return Optional.empty();
        }

        private NestedTree remove(PostFilterTree postFilterTree) {
            if (postFilterTree.isEmpty()) {
                return this;
            }
            var modified = remove(tree, postFilterTree.flattenedConditions());
            return new NestedTree(modified == null ? new Any.EmptyString() : modified);
        }

        private static Node remove(Node tree, Collection<Condition> remove) {
            if (remove.stream().anyMatch(n -> n == tree)) {
                return null;
            }
            if (tree instanceof Group g) {
                return g.mapFilterAndReinstantiate(c -> remove(c, remove), Objects::nonNull);
            }
            if (tree instanceof Not(Node node)) {
                var removed = remove(node, remove);
                return removed != null ? new Not(removed) : null;
            }
            return tree;
        }
    }

    private record PostFilterTree(Node tree) {
        private static PostFilterTree from(ExpandedQueryTree eqt, NestedTree nestedTree, SelectedFacets selectedFacets) {
            if (selectedFacets == null) {
                return new PostFilterTree(null);
            }

            List<List<Node>> multiSelectGroups = selectedFacets.getAllMultiOrRadioSelected()
                    .values()
                    .stream()
                    .map(origSelected -> origSelected.stream().map(eqt.nodeMap()::get).toList())
                    .toList();

            if (multiSelectGroups.isEmpty()) {
                return new PostFilterTree(null);
            }

            List<Nested> nestedGroups = new QueryTree(nestedTree.tree()).getTopNodesOfType(Nested.class);

            List<Node> postFilterNodes = new ArrayList<>();
            for (List<Node> multiSelected : multiSelectGroups) {
                var selectedConditions = multiSelected.stream()
                        .map(PostFilterTree::flattenedConditions)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
                nestedGroups.stream()
                        .filter(nested -> intersect(flattenedConditions(nested), selectedConditions))
                        .findFirst()
                        .ifPresentOrElse(postFilterNodes::add,
                                () -> postFilterNodes.add(multiSelected.size() > 1 ? new Or(multiSelected) : multiSelected.getFirst()));

            }

            return new PostFilterTree(postFilterNodes.size() > 1 ? new And(postFilterNodes, false) : postFilterNodes.getFirst());
        }

        private boolean isEmpty() {
            return tree == null;
        }

        private Set<Condition> flattenedConditions() {
            return flattenedConditions(tree);
        }

        private static Set<Condition> flattenedConditions(Node node) {
            return node.allDescendants().filter(Condition.class::isInstance).map(Condition.class::cast).collect(Collectors.toSet());
        }

        private static boolean intersect(Set<?> a, Set<?> b) {
            return a.stream().anyMatch(b::contains);
        }
    }

    private static final class Nested extends And {
        private final String stem;

        private Nested(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.getCoreEsQuery(esSettings));
        }

        @Override
        public Nested newInstance(List<Node> children) {
            return new Nested(children, stem);
        }
    }
}

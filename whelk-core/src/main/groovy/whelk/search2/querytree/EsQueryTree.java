package whelk.search2.querytree;

import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.SelectedFacets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
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
        static NestedTree from(ExpandedQueryTree queryTree, EsMappings esMappings) {
            return new NestedTree(getNestedTree(queryTree.tree(), esMappings));
        }

        NestedTree remove(PostFilterTree postFilterTree) {
            if (postFilterTree.isEmpty()) {
                return this;
            }
            var modified = remove(tree, flattenedConditions(postFilterTree.tree()));
            return new NestedTree(modified == null ? new Any.EmptyString() : modified);
        }

        private static Node getNestedTree(Node tree, EsMappings esMappings) {
            if (tree instanceof And and) {
                tree = regroupAltSelectorsByNestedStem(and, esMappings);
            }
            return _getNestedTree(tree, esMappings);
        }

        private static Node _getNestedTree(Node tree, EsMappings esMappings) {
            if (tree instanceof And and) {
                List<NestedAnd> nestedAndGroups = collectNestedGroups(and, esMappings);
                List<Node> nested = nestedAndGroups.stream().map(And::children).flatMap(List::stream).toList();
                List<Node> nonNested = and.children().stream()
                        .filter(Predicate.not(nested::contains))
                        .map(n -> getNestedTree(n, esMappings)).toList();
                if (nestedAndGroups.size() == 1 && nonNested.isEmpty()) {
                    return nestedAndGroups.getFirst();
                }
                return new And(Stream.concat(nestedAndGroups.stream(), nonNested.stream()).toList(), false);
            }
            if (tree instanceof Or or) {
                var nestedOr = getNestedStem(or, esMappings)
                        .filter(esMappings::isNestedNotInParentField)
                        .map(stem -> new NestedOr(or.children(), stem));
                return nestedOr.isPresent()
                        ? nestedOr.get()
                        : or.mapAndReinstantiate(n -> getNestedTree(n, esMappings));
            }
            return tree;
        }

        private static List<NestedAnd> collectNestedGroups(And and, EsMappings esMappings) {
            List<NestedAnd> nestedAndGroups = new ArrayList<>();

            Map<String, Map<String, List<Node>>> groups = new LinkedHashMap<>();

            Runnable harvestNested = () -> {
                groups.forEach((stem, nodesByField) -> {
                    var group = nodesByField.values().stream().flatMap(List::stream).toList();
                    if (group.size() > 1) {
                        nestedAndGroups.add(new NestedAnd(group, stem));
                    }
                });
                groups.clear();
            };

            and.children().forEach(n -> {
                var stem = getNestedStem(n, esMappings);
                if (stem.isEmpty()) {
                    harvestNested.run();
                } else {
                    var selectors = flattenedConditions(n)
                            .stream()
                            .map(Condition::selector)
                            .distinct()
                            .toList();
                    for (Selector s : selectors) {
                        var field = s.esField();
                        var nodesForField = groups.getOrDefault(stem.get(), Map.of()).getOrDefault(field, List.of());
                        if (!nodesForField.isEmpty() && !s.isLdSetContainer()) {
                            harvestNested.run();
                        }
                        groups.computeIfAbsent(stem.get(), k -> new LinkedHashMap<>())
                                .computeIfAbsent(field, k -> new ArrayList<>())
                                .add(n);
                    }
                }
            });

            harvestNested.run();

            return nestedAndGroups;
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
            if (node instanceof Or or) {
                var groupedByNestedStem = or.children().stream()
                        .collect(Collectors.groupingBy(n -> getNestedStem(n, esMappings)));
                if (groupedByNestedStem.size() == 1) {
                    return groupedByNestedStem.keySet().iterator().next();
                }
            }
            return Optional.empty();
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
        static PostFilterTree from(ExpandedQueryTree eqt, NestedTree nestedTree, SelectedFacets selectedFacets) {
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

            List<NestedAnd> nestedAndGroups = new QueryTree(nestedTree.tree()).getTopNodesOfType(NestedAnd.class);

            List<Node> postFilterNodes = new ArrayList<>();
            for (List<Node> multiSelected : multiSelectGroups) {
                var selectedConditions = multiSelected.stream()
                        .map(EsQueryTree::flattenedConditions)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
                nestedAndGroups.stream()
                        .filter(nestedAnd -> intersect(flattenedConditions(nestedAnd), selectedConditions))
                        .findFirst()
                        .ifPresentOrElse(postFilterNodes::add,
                                () -> postFilterNodes.add(multiSelected.size() > 1 ? new Or(multiSelected) : multiSelected.getFirst()));

            }

            return new PostFilterTree(postFilterNodes.size() > 1 ? new And(postFilterNodes, false) : postFilterNodes.getFirst());
        }

        boolean isEmpty() {
            return tree == null;
        }

        private static boolean intersect(Set<?> a, Set<?> b) {
            return a.stream().anyMatch(b::contains);
        }
    }

    static Set<Condition> flattenedConditions(Node node) {
        return node.allDescendants().filter(Condition.class::isInstance).map(Condition.class::cast).collect(Collectors.toSet());
    }

    private static final class NestedAnd extends And {
        private final String stem;

        NestedAnd(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.getCoreEsQuery(esSettings));
        }

        @Override
        public NestedAnd newInstance(List<Node> children) {
            return new NestedAnd(children, stem);
        }
    }

    private static final class NestedOr extends Or {
        private final String stem;

        NestedOr(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.getCoreEsQuery(esSettings));
        }

        @Override
        public NestedOr newInstance(List<Node> children) {
            return new NestedOr(children, stem);
        }
    }
}

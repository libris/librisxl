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
            return switch (tree) {
                case And and -> {
                    List<NestedAnd> nestedAndGroups = collectNestedGroups(and, esMappings);
                    List<Node> nested = nestedAndGroups.stream().map(And::children).flatMap(List::stream).toList();
                    List<Node> nonNested = and.children().stream()
                            .filter(Predicate.not(nested::contains))
                            .map(n -> getNestedTree(n, esMappings)).toList();
                    if (nestedAndGroups.size() == 1 && nonNested.isEmpty()) {
                        yield nestedAndGroups.getFirst();
                    }
                    yield new And(Stream.concat(nestedAndGroups.stream(), nonNested.stream()).toList(), false);
                }
                case Or or -> {
                    var nestedOr = findUnambiguousNestedStem(or, esMappings)
                            .filter(esMappings::isNestedNotInParentField)
                            .map(stem -> new NestedOr(or.children(), stem));
                    yield nestedOr.isPresent()
                            ? nestedOr.get()
                            : or.mapAndReinstantiate(n -> getNestedTree(n, esMappings));
                }
                case Not(Node node) -> node instanceof Group g
                        ? getNestedTree(g.getInverse(), esMappings)
                        : new Not(getNestedTree(node, esMappings));
                case Condition c -> c.selector().getEsNestedStem(esMappings)
                        .filter(esMappings::isNestedNotInParentField)
                        .map(stem -> (Condition) new NestedCondition(c, stem))
                        .orElse(c);
                default -> tree;
            };
        }

        private static List<NestedAnd> collectNestedGroups(And and, EsMappings esMappings) {
            List<NestedAnd> nestedAndGroups = new ArrayList<>();

            Map<String, Map<String, List<Node>>> groups = new LinkedHashMap<>();

            Runnable harvestNested = () -> {
                groups.forEach((stem, nodesByField) -> {
                    var group = nodesByField.values().stream().flatMap(List::stream).distinct().toList();
                    if (group.size() > 1 && !group.stream().allMatch(Not.class::isInstance)) {
                        nestedAndGroups.add(new NestedAnd(group, stem));
                    }
                });
                groups.clear();
            };

            and.children().forEach(n -> {
                var stem = findUnambiguousNestedStem(n, esMappings);
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
                        if (!nodesForField.isEmpty() && !s.isLdSetContainer() && !(n instanceof Not)) {
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

        private static Optional<String> findUnambiguousNestedStem(Node node, EsMappings esMappings) {
            return switch (node) {
                case Condition c -> c.selector().getEsNestedStem(esMappings);
                case Not not -> findUnambiguousNestedStem(not.node(), esMappings);
                case Or or -> {
                    var stems = or.children().stream()
                            .map(n -> findUnambiguousNestedStem(n, esMappings))
                            .collect(Collectors.toSet());
                    yield stems.size() == 1
                            ? stems.iterator().next()
                            : Optional.empty();
                }
                default -> Optional.empty();
            };
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

            List<NestedNode> nestedNodes = new QueryTree(nestedTree.tree()).getTopNodesOfType(NestedNode.class);

            List<Node> postFilterNodes = new ArrayList<>();
            for (List<Node> multiSelected : multiSelectGroups) {
                var selectedConditions = multiSelected.stream()
                        .map(EsQueryTree::flattenedConditions)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
                nestedNodes.stream()
                        .map(Node.class::cast)
                        .filter(nestedNode -> intersects(flattenedConditions(nestedNode), selectedConditions))
                        .findFirst()
                        .ifPresentOrElse(postFilterNodes::add,
                                () -> postFilterNodes.add(multiSelected.size() > 1 ? new Or(multiSelected) : multiSelected.getFirst()));

            }

            return new PostFilterTree(postFilterNodes.size() > 1 ? new And(postFilterNodes, false) : postFilterNodes.getFirst());
        }

        boolean isEmpty() {
            return tree == null;
        }

        private static boolean intersects(Set<?> a, Set<?> b) {
            return a.stream().anyMatch(b::contains);
        }
    }

    static Set<Condition> flattenedConditions(Node node) {
        return node.allDescendants().filter(Condition.class::isInstance).map(Condition.class::cast).collect(Collectors.toSet());
    }

    private sealed interface NestedNode permits NestedCondition, NestedAnd, NestedOr {
    }

    private static final class NestedCondition extends Condition implements NestedNode {
        private final String stem;

        NestedCondition(Condition c, String stem) {
            super(c.selector(), c.operator(), c.value());
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.toEs(esSettings));
        }
    }

    private static final class NestedAnd extends And implements NestedNode {
        private final String stem;

        NestedAnd(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.toEs(esSettings));
        }

        @Override
        public NestedAnd newInstance(List<Node> children) {
            return new NestedAnd(children, stem);
        }
    }

    private static final class NestedOr extends Or implements NestedNode {
        private final String stem;

        NestedOr(List<? extends Node> children, String stem) {
            super(children);
            this.stem = stem;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            return nestedWrap(stem, super.toEs(esSettings));
        }

        @Override
        public NestedOr newInstance(List<Node> children) {
            return new NestedOr(children, stem);
        }
    }
}

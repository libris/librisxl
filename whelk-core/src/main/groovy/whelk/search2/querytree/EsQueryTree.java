package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
import whelk.search2.Operator;
import whelk.search2.SelectedFacets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
    private final MainQueryTree mainQueryTree;
    private final PostFilterTree postFilterTree;

    public EsQueryTree(ExpandedQueryTree queryTree, ESSettings esSettings) {
        this(queryTree, esSettings, null);
    }

    public EsQueryTree(ExpandedQueryTree queryTree, ESSettings esSettings, SelectedFacets selectedFacets) {
        this.esSettings = esSettings;
        this.mainQueryTree = MainQueryTree.from(queryTree, esSettings.mappings());
        this.postFilterTree = PostFilterTree.from(queryTree, mainQueryTree, selectedFacets);
    }

    public Map<String, Object> getMainQuery() {
        return mainQueryTree.remove(postFilterTree).tree().toEs(esSettings);
    }

    public Map<String, Object> getPostFilter() {
        return postFilterTree.isEmpty() ? Map.of() : postFilterTree.tree().toEs(esSettings);
    }

    @Override
    public String toString() {
        return mainQueryTree.tree().toString();
    }

    private record MainQueryTree(Node tree) {
        static MainQueryTree from(ExpandedQueryTree queryTree, EsMappings esMappings) {
            var nodeTree = restructureForEs(queryTree.tree(), invertMap(queryTree.nodeMap()), esMappings);
            return new MainQueryTree(nodeTree);
        }

        MainQueryTree remove(PostFilterTree postFilterTree) {
            if (postFilterTree.isEmpty()) {
                return this;
            }
            var modified = remove(tree, flattenedConditions(postFilterTree.tree()));
            return new MainQueryTree(modified == null ? new Any.EmptyString() : modified);
        }

        private static Node restructureForEs(Node tree, Map<Node, Node> nodeMap, EsMappings esMappings) {
            return switch (tree) {
                case And and -> {
                    List<NestedAnd> nestedAndGroups = collectNestedGroups(and, esMappings);
                    List<Node> nested = nestedAndGroups.stream().map(And::children).flatMap(List::stream).toList();
                    List<Node> nonNested = and.children().stream()
                            .filter(Predicate.not(nested::contains))
                            .map(n -> restructureForEs(n, nodeMap, esMappings)).toList();
                    if (nestedAndGroups.size() == 1 && nonNested.isEmpty()) {
                        yield nestedAndGroups.getFirst();
                    }
                    yield new And(Stream.concat(nestedAndGroups.stream(), nonNested.stream()).toList(), false);
                }
                case Or or -> {
                    var nestedOr = findUnambiguousNestedStem(or, esMappings)
                            .filter(esMappings::isNestedNotInParentField)
                            .map(stem -> new NestedOr(or.children(), stem));
                    var restructuredOr = nestedOr.isPresent()
                            ? nestedOr.get()
                            : (Or) or.mapAndReinstantiate(n -> restructureForEs(n, nodeMap, esMappings));
                    yield nodeMap.get(or) instanceof Condition c
                            && c.selector().isComposite()
                            && restructuredOr.children().stream().map(Object::getClass).allMatch(Condition.class::equals)
                            ? new MultiCondition(c, restructuredOr)
                            : restructuredOr;
                }
                case Not(Node node) -> node instanceof Group g
                        ? restructureForEs(g.getInverse(), nodeMap, esMappings)
                        : new Not(restructureForEs(node, nodeMap, esMappings));
                case Condition c -> c.selector().getEsNestedStem(esMappings)
                        .filter(stem -> esMappings.isNestedNotInParentField(stem)
                                || c.value().isMultiToken()
                                || c.operator() == Operator.LIKE)
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

        private static Map<Node, Node> invertMap(Map<Node, Node> nodeMap) {
            Map<Node, Node> inverted = new HashMap<>();
            nodeMap.forEach((k, v) -> inverted.put(v, k));
            return inverted;
        }
    }

    private record PostFilterTree(Node tree) {
        static PostFilterTree from(ExpandedQueryTree eqt, MainQueryTree mainQueryTree, SelectedFacets selectedFacets) {
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

            List<NestedNode> nestedNodes = new QueryTree(mainQueryTree.tree()).getTopNodesOfType(NestedNode.class);

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

        String stem() {
            return stem;
        }
    }

    private static final class MultiCondition extends Condition {
        private final Or expanded;

        MultiCondition(Condition origCondition, Or expanded) {
            super(origCondition.selector(), origCondition.operator(), origCondition.value());
            this.expanded = expanded;
        }

        @Override
        public Map<String, Object> toEs(ESSettings esSettings) {
            if (value() instanceof FreeText ft) {
                Map<String, Object> esQuery = esFreeTextFilter(collectFields(expanded), ft, esSettings);
                return expanded instanceof NestedOr nOr ? nestedWrap(nOr.stem(), esQuery) : esQuery;
            }
            return expanded.toEs(esSettings);
        }

        private Map<String, Object> esFreeTextFilter(List<String> fields, FreeText ft, ESSettings esSettings) {
            var boostSettings = esSettings.boost().fieldBoost();
            return ft.toEs(boostSettings.withFields(fields));
        }

        private static List<String> collectFields(Or or) {
            return or.children().stream()
                    .map(Condition.class::cast)
                    .map(Condition::selector)
                    .map(s -> s.isObjectProperty()
                            ? String.format("%s.%s", s.esField(), JsonLd.SEARCH_KEY)
                            : s.esField())
                    .toList();
        }
    }
}

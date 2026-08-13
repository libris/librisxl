package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.search2.Operator.EQUALS;

public class QueryTreeExpander {
    public static Node expand(Node queryTreeNode, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return switch (queryTreeNode) {
            case Condition c -> expandCondition(c, jsonLd, rdfSubjectTypes);
            case And and -> expandAnd(and, jsonLd, rdfSubjectTypes);
            case Or or -> expandOr(or, jsonLd, rdfSubjectTypes);
            case Not not -> expandNot(not, jsonLd, rdfSubjectTypes);
            case FilterAlias fa -> expand(fa.getParsed(), jsonLd, rdfSubjectTypes);
            default -> queryTreeNode;
        };
    }

    private static Node expandCondition(Condition condition, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (condition.selector().isValid()) {
            return expandSelector(condition, jsonLd, rdfSubjectTypes)
                    .deepMap(QueryTreeExpander::expandRestrictions)
                    .deepMap(n -> expandType(n, jsonLd));
        }
        return condition;
    }

    private static Node expandOr(Or or, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return Node.withMappedChildren(or, child -> expand(child, jsonLd, rdfSubjectTypes));
    }

    private static Node expandAnd(And and, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        List<String> innerTypes = RdfSubjectType.extractFrom(and).typeNames();

        Collection<String> rulingTypes = innerTypes.isEmpty()
                ? rdfSubjectTypes
                : innerTypes;

        return Node.withMappedChildren(and, child -> expand(child, jsonLd, rulingTypes));
    }

    private static Node expandNot(Not not, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (not.node() instanceof FilterAlias) {
            return null;
        }
        return new Not(expand(not.node(), jsonLd, rdfSubjectTypes));
    }

    private static Node expandSelector(Condition condition, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        List<Condition> altConditions = expandSelector(condition.selector(), jsonLd, rdfSubjectTypes, true)
                .stream()
                .map(condition::withSelector)
                .toList();

        return altConditions.size() == 1
                ? altConditions.getFirst()
                : new QueryTree.ExpandedTree.DerivedOr(altConditions, condition);
    }

    private static List<Selector> expandSelector(Selector selector, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        return switch (selector) {
            case Key k -> List.of(k);
            case Property p -> expandProperty(p, jsonLd, rdfSubjectTypes, allowIncompatible);
            case Path path -> expandPath(path, jsonLd, rdfSubjectTypes, allowIncompatible);
        };
    }

    public static List<Selector> expandProperty(Property property, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        return switch (property) {
            case Property.CompositeProperty cp ->
                    expandCompositeProperty(cp, jsonLd, rdfSubjectTypes, allowIncompatible);
            case Property.ShorthandProperty sp ->
                    expandShorthandProperty(sp, jsonLd, rdfSubjectTypes, allowIncompatible);
            default -> {
                if (rdfSubjectTypes.isEmpty() || property.isRdfType()) {
                    yield List.of(property);
                }

                List<List<Property>> altPaths = expandPropertyByRdfSubjectTypes(property, jsonLd, rdfSubjectTypes);

                if (altPaths.isEmpty() && allowIncompatible) {
                    altPaths.add(List.of(property));
                }

                yield altPaths.stream()
                        .filter(Predicate.not(List::isEmpty))
                        .map(altPath -> altPath.size() > 1 ? new Path(altPath) : altPath.getFirst())
                        .toList();
            }
        };
    }

    private static List<Selector> expandCompositeProperty(Property.CompositeProperty property, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        return property.getComponents(jsonLd)
                .stream()
                .flatMap(s -> expandSelector(s, jsonLd, rdfSubjectTypes, allowIncompatible).stream())
                .distinct()
                .toList();
    }

    private static List<Selector> expandShorthandProperty(Property.ShorthandProperty property, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        return expandPath(new Path(property.path()), jsonLd, rdfSubjectTypes, allowIncompatible);
    }

    private static List<List<Property>> expandPropertyByRdfSubjectTypes(Property property, JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        Set<Property> integralRelations = rdfSubjectTypes.stream()
                .map(t -> QueryUtil.getIntegralRelationsForType(t, jsonLd))
                .flatMap(List::stream)
                .collect(Collectors.toSet());

        Predicate<Property> followIntegralRelation = integralProp ->
                integralProp.range().stream().anyMatch(irRangeType -> property.mayAppearOnType(irRangeType, jsonLd));

        List<List<Property>> altPaths = new ArrayList<>();

        // Add alternative paths with compatible integral relations prepended
        integralRelations.stream()
                .filter(followIntegralRelation)
                .map(ir -> Stream.concat(Stream.of(ir), property.path().stream()).toList())
                .forEach(altPaths::add);

        // Include the original property too, if compatible with the queried subject types
        if (rdfSubjectTypes.stream().anyMatch(t -> property.mayAppearOnType(t, jsonLd))) {
            altPaths.add(List.of(property));
        }

        return altPaths;
    }

    private static List<Selector> expandPath(Path path, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        List<Selector> altPaths = new ArrayList<>();

        expandPath(path.path(), jsonLd, rdfSubjectTypes, allowIncompatible)
                .forEach(l -> {
                    if (l.size() == 1) {
                        altPaths.add(l.getFirst());
                    } else if (l.size() > 1) {
                        altPaths.add(new Path(l));
                    }
                });

        return altPaths;
    }

    private static List<List<PathElement>> expandPath(List<? extends PathElement> tail, JsonLd jsonLd, Collection<String> rdfSubjectTypes, boolean allowIncompatible) {
        if (tail.isEmpty()) {
            return List.of(List.of());
        }
        PathElement next = tail.getFirst();
        List<? extends PathElement> newTail = tail.subList(1, tail.size());
        List<Selector> nextExpanded = expandSelector(next, jsonLd, rdfSubjectTypes, allowIncompatible);

        var shouldRecurse = (!newTail.isEmpty() && newTail.getFirst().isComposite())
                || (next instanceof Property p && "hasItem".equals(p.name()));

        if (shouldRecurse) {
            return nextExpanded.stream()
                    .flatMap(s -> expandPath(newTail, jsonLd, next.range(), allowIncompatible).stream()
                            .map(altPath -> concat(s.path(), altPath)))
                    .toList();
        }

        return nextExpanded.stream()
                .map(s -> concat(s.path(), newTail))
                .toList();
    }

    private static List<PathElement> concat(List<? extends PathElement> left, List<? extends PathElement> right) {
        return Stream.concat(left.stream(), right.stream()).toList();
    }

    private static Node expandRestrictions(Node node) {
        if (node instanceof Condition c) {
            List<Condition> restrictions = buildRestrictions(c.selector().path());
            return restrictions.isEmpty()
                    ? node
                    : new And(Stream.concat(Stream.of(node), restrictions.stream()).toList());
        }
        return node;
    }

    private static List<Condition> buildRestrictions(List<? extends PathElement> path) {
        List<Condition> conditions = new ArrayList<>();
        List<PathElement> currentPath = new ArrayList<>();
        for (PathElement pe : path) {
            currentPath.add(pe);
            if (pe instanceof Property.RestrictedSubProperty p && !p.hasIndexKey()) {
                for (Restrictions.HasValue r : p.getObjectRestrictions()) {
                    var restrictedPath = new Path(Stream.concat(currentPath.stream(), r.onProperty().path().stream()).toList());
                    conditions.add(new Condition(restrictedPath, EQUALS, r.value()));
                }
            }
        }
        return conditions;
    }

    // When querying type, match any subclass by default (TODO: make this optional)
    private static Node expandType(Node node, JsonLd jsonLd) {
        if (!(node instanceof Condition c
                && c.selector().isType()
                && c.value() instanceof VocabTerm v)) {
            return node;
        }

        String baseType = v.key();

        Set<String> subtypes = jsonLd.getSubClasses(baseType);
        if (subtypes.isEmpty()) {
            return node;
        }

        List<Condition> altTypes = Stream.concat(Stream.of(baseType), subtypes.stream())
                .filter(Predicate.not(jsonLd::isDeprecated))
                .sorted()
                .map(t -> c.withValue(new VocabTerm(t, jsonLd.vocabIndex.get(t))))
                .toList();

        return new QueryTree.ExpandedTree.DerivedOr(altTypes, c);
    }
}

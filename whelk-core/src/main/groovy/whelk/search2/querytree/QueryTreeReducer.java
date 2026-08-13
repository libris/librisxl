package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

public class QueryTreeReducer {
    public static Node reduce(Node queryTreeNode, JsonLd jsonLd) {
        return switch (queryTreeNode) {
            case And and -> reduceAnd(and, jsonLd);
            case Or or -> reduceOr(or, jsonLd);
            default -> queryTreeNode;
        };
    }

    private static Node reduceAnd(And and, JsonLd jsonLd) {
        return reduceByImplication(and, jsonLd, (implying, implied) -> implying);
    }

    private static Node reduceOr(Or or, JsonLd jsonLd) {
        return reduceByImplication(or, jsonLd, (implying, implied) -> implied);
    }

    private static Node reduceByImplication(
            Group group,
            JsonLd jsonLd,
            BinaryOperator<Node> pick) {

        return reduce(group, jsonLd, (a, b) -> {
            if (implies(a, b, jsonLd)) {
                return Optional.of(pick.apply(a, b));
            }
            if (implies(b, a, jsonLd)) {
                return Optional.of(pick.apply(b, a));
            }
            return Optional.empty();
        });
    }

    private static Node reduce(Group group, JsonLd jsonLd, BiFunction<Node, Node, Optional<Node>> merge) {
        List<Node> reduced = new ArrayList<>();

        group.children()
                .stream()
                .map(child -> reduce(child, jsonLd))
                .forEach(child -> {
                    for (int i = 0; i < reduced.size(); i++) {
                        Optional<Node> merged = merge.apply(child, reduced.get(i));
                        if (merged.isPresent()) {
                            reduced.set(i, merged.get());
                            return;
                        }
                    }
                    reduced.add(child);
                });

        return reduced.size() == 1
                ? reduced.getFirst()
                : group.newInstance(reduced, true);
    }

    public static boolean implies(Node a, Node b, JsonLd jsonLd) {
        return switch (a) {
            case And and -> andImplies(and, b, jsonLd);
            case Or or -> orImplies(or, b, jsonLd);
            case Not not -> notImplies(not, b, jsonLd);
            case FilterAlias alias -> filterAliasImplies(alias, b, jsonLd);
            case Condition.Type type -> typeImplies(type, b, jsonLd);
            default -> matches(b, a::equals);
        };
    }

    private static boolean andImplies(And and, Node b, JsonLd jsonLd) {
        return b instanceof And bAnd
                ? bAnd.children().stream().allMatch(child -> implies(and, child, jsonLd))
                : and.children().stream().anyMatch(child -> implies(child, b, jsonLd));
    }

    private static boolean orImplies(Or or, Node b, JsonLd jsonLd) {
        return or.children().stream().allMatch(child -> implies(child, b, jsonLd));
    }

    private static boolean notImplies(Not not, Node b, JsonLd jsonLd) {
        if (not.node() instanceof FilterAlias || b instanceof Not(FilterAlias fa)) {
            return not.equals(b);
        }

        Node inverse = b.getInverse();
        return !(inverse instanceof Not) && implies(inverse, not.node(), jsonLd);
    }

    private static boolean filterAliasImplies(FilterAlias alias, Node b, JsonLd jsonLd) {
        return matches(b, alias::equals) || implies(alias.getParsed(), b, jsonLd);
    }

    private static boolean typeImplies(Condition.Type type, Node b, JsonLd jsonLd) {
        return matches(b, node -> node instanceof Condition.Type other
                && jsonLd.isSubClassOf(type.type(), other.type()));
    }

    private static boolean matches(Node node, Predicate<Node> predicate) {
        return switch (node) {
            case And and -> and.children().stream().allMatch(predicate);
            case Or or -> or.children().stream().anyMatch(predicate);
            default -> predicate.test(node);
        };
    }
}

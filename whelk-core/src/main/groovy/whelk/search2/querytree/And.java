package whelk.search2.querytree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static whelk.search2.QueryUtil.mustWrap;

public record And(List<Node> children) implements Group {
    @Override
    public Group create(List<Node> children) {
        return new And(children);
    }

    @Override
    public String delimiter() {
        return " ";
    }

    @Override
    public String key() {
        return "and";
    }

    @Override
    public Map<String, Object> wrap(List<Map<String, Object>> esChildren) {
        return mustWrap(esChildren);
    }

    public boolean contains(Node node) {
        return switch (node) {
            case And and -> new HashSet<>(children).containsAll(and.children());
            default -> children.contains(node);
        };
    }

    public Node remove(Node node) {
        if (!contains(node)) {
            return this;
        }
        var newConjuncts = switch (node) {
            case And and -> filter(Predicate.not(and.children()::contains));
            default -> filter(Predicate.not(node::equals));
        };
        return switch (newConjuncts.size()) {
            case 0 -> null;
            case 1 -> newConjuncts.getFirst();
            default -> new And(newConjuncts);
        };
    }

    public And add(Node node) {
        if (contains(node)) {
            return this;
        }
        var newConjuncts = new ArrayList<>(children);
        switch (node) {
            case And and -> and.children().forEach(c -> {
                if (!newConjuncts.contains(c)) {
                    newConjuncts.add(c);
                }
            });
            default -> newConjuncts.add(node);
        }
        return new And(newConjuncts);
    }

    public Node replace(Node old, Node replacement) {
        Predicate<Node> removeOld = switch (old) {
            case And and -> Predicate.not(and::contains);
            default -> Predicate.not(old::equals);
        };

        List<Node> newConjuncts = filter(removeOld);

        switch (replacement) {
            case And and -> newConjuncts.addAll(and.children());
            default -> newConjuncts.add(replacement);
        }

        return newConjuncts.size() == 1 ? newConjuncts.getFirst() : new And(newConjuncts);
    }
}

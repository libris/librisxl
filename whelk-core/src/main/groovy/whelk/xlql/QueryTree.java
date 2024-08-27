package whelk.xlql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import java.util.stream.StreamSupport;

import static whelk.xlql.Operator.EQUALS;

import static whelk.xlql.Disambiguate.RDF_TYPE;

public class QueryTree {
    public sealed interface Node permits And, Or, Nested, Field, FreeText {
        default List<Node> children() {
            return Collections.emptyList();
        }
    }

    public record And(List<Node> conjuncts) implements Node {
        @Override
        public List<Node> children() {
            return conjuncts;
        }
    }

    public record Or(List<Node> disjuncts) implements Node {
        public List<Node> children() {
            return disjuncts;
        }
    }

    public record Nested(List<Field> fields, Operator operator, String stem) implements Node {
    }

    public record Field(String path, Operator operator, String value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
        public boolean isWild() {
            return operator == EQUALS && Operator.WILDCARD.equals(value);
        }
    }

    private final Set<String> esNestedFields;

    public Node tree;

    public QueryTree(Node tree, Set<String> esNestedFields) {
        this.tree = tree;
        this.esNestedFields = esNestedFields;
    }

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate, Disambiguate.OutsetType outsetType, Set<String> esNestedFields) {
        this.esNestedFields = esNestedFields;
        this.tree = sqtToQt(sqt.tree, disambiguate, outsetType);
    }

    /**
     * There is no freetext or all freetext nodes are "*"
     */
    public boolean isWild() {
        return StreamSupport.stream(allDescendants(tree).spliterator(), false)
                .noneMatch(n -> n instanceof FreeText && !((FreeText) n).isWild());
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

        return () -> i;
    }

    private Node sqtToQt(SimpleQueryTree.Node sqtNode, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        return switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                // TODO: merge nested fields? E.g. itemHeldBy + hasItem.x
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .map(c -> sqtToQt(c, disambiguate, outset))
                        .toList();
                yield new And(conjuncts);
            }
            case SimpleQueryTree.Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(d -> sqtToQt(d, disambiguate, outset))
                        .toList();
                yield new Or(disjuncts);
            }
            case SimpleQueryTree.FreeText ft -> new FreeText(ft.operator(), ft.value());
            case SimpleQueryTree.PropertyValue pv -> buildField(pv, disambiguate, outset);
            case SimpleQueryTree.BoolFilter ignored -> throw new RuntimeException("Failed to create QueryTree. Tree must not contain unexpanded bool filters."); // Should never be reached.
        };
    }

    private Node buildField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        boolean isAccuratePath = pv.path().size() > 1;

        Path path = new Path(pv.path());
        Operator operator = pv.operator();
        String value = pv.value().string();

        if (isAccuratePath) {
            return new Field(path.stringify(), operator, value);
        }

        if (RDF_TYPE.equals(pv.property())) {
            return buildTypeField(pv, path.stringify(), disambiguate);
        }

        path.expand(pv.property(), disambiguate, outset, pv.value());

        List<Node> altFields = new ArrayList<>();
        for (Path stem : path.getAltStems()) {
            var fields = path.branches.isEmpty()
                    ? List.of(new Field(stem.stringify(), operator, value))
                    : path.branches.stream()
                    .map(b -> new Field(stem.attachBranch(b).stringify(), operator, b.value().string()))
                    .toList();
            var nested = getNestedPath(stem.stringify());
            if (nested.isPresent()) {
                altFields.add(new Nested(fields, operator, nested.get()));
            } else {
                altFields.add(fields.size() > 1
                        ? new And(fields.stream().map(Node.class::cast).toList())
                        : fields.getFirst());
            }
        }

        if (altFields.size() == 1) {
            return altFields.getFirst();
        }

        return operator == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    private static Node buildTypeField(SimpleQueryTree.PropertyValue pv, String path, Disambiguate disambiguate) {
        Set<String> altTypes = "Work".equals(pv.value().string())
                ? disambiguate.workTypes
                : ("Instance".equals(pv.value().string()) ? disambiguate.instanceTypes : Collections.emptySet());

        if (altTypes.isEmpty()) {
            return new Field(path, pv.operator(), pv.value().string());
        }

        List<Node> altFields = altTypes.stream()
                .sorted()
                .map(type -> (Node) new Field(path, pv.operator(), type))
                .toList();

        return pv.operator() == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    private Optional<String> getNestedPath(String path) {
        if (esNestedFields.contains(path)) {
            return Optional.of(path);
        }
        return esNestedFields.stream().filter(path::startsWith).findFirst();
    }
}

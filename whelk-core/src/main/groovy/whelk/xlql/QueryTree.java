package whelk.xlql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static whelk.xlql.Operator.EQUALS;

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

    public record Nested(List<Field> fields, Operator operator) implements Node {
    }

    public record Field(Path path, Operator operator, String value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
        public boolean isWild() {
            return operator == EQUALS && "*".equals(value);
        }
    }

    public Node tree;

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate) {
        this.tree = sqtToQt(sqt.tree, disambiguate, Disambiguate.OutsetType.RESOURCE);
    }

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate, Disambiguate.OutsetType outsetType) {
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

    private static Node sqtToQt(SimpleQueryTree.Node sqtNode, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        return switch (sqtNode) {
            case SimpleQueryTree.And and -> {
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
        };
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        boolean isAccuratePath = pv.propertyPath().size() > 1;

        Path path = new Path(pv.propertyPath());
        Operator operator = pv.operator();
        String value = pv.value().string();

        if (disambiguate.isObjectProperty(pv.property())) {
            switch (pv.value()) {
                case SimpleQueryTree.Link ignored -> path.appendId();
                case SimpleQueryTree.Literal ignored -> path.appendUnderscoreStr();
                case SimpleQueryTree.VocabTerm ignored -> {}
            }
        }

        if (isAccuratePath) {
            return new Field(path, operator, value);
        }

        List<Node> altFields = path.expand(pv.property(), disambiguate, outset)
                .stream()
                .map(p -> newFields(pv, p, disambiguate))
                .collect(Collectors.toList());

        if (altFields.size() == 1) {
            return altFields.getFirst();
        }

        return operator == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    static Node newFields(SimpleQueryTree.PropertyValue pv, Path path, Disambiguate disambiguate) {
        if ("rdf:type".equals(pv.property())) {
            return buildTypeField(pv, path, disambiguate);
        }

        Field f = new Field(path, pv.operator(), pv.value().string());

        if (path.defaultFields.isEmpty()) {
            return f;
        }

        List<Field> fields = new ArrayList<>(List.of(f));

        path.defaultFields.forEach(df -> {
                    Path dfPath = new Path(df.path());
                    String property = df.path().getLast();
                    if (disambiguate.isObjectProperty(property) && !disambiguate.hasVocabValue(property)) {
                        dfPath.appendId();
                    }
                    fields.add(new Field(dfPath, pv.operator(), df.value()));
                }
        );

        return new Nested(fields, pv.operator());
    }

    private static Node buildTypeField(SimpleQueryTree.PropertyValue pv, Path path, Disambiguate disambiguate) {
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
}

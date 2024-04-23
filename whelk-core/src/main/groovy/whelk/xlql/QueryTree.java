package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class QueryTree {
    public sealed interface Node permits And, Or, Nested, Field, FreeText {
    }

    public record And(List<Node> conjuncts) implements Node {
    }

    public record Or(List<Node> disjuncts) implements Node {
    }

    public record Nested(List<Field> fields, Operator operator) implements Node {
    }

    public record Field(Path path, Operator operator, String value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
    }

    public Node tree;

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate) {
        this.tree = sqtToQt(sqt.tree, disambiguate, Disambiguate.OutsetType.RESOURCE);
    }

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate, Disambiguate.OutsetType outsetType) {
        this.tree = sqtToQt(sqt.tree, disambiguate, outsetType);
    }

    private static Node sqtToQt(SimpleQueryTree.Node sqtNode, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .map(c -> sqtToQt(c, disambiguate, outset))
                        .toList();
                return new And(conjuncts);
            }
            case SimpleQueryTree.Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(d -> sqtToQt(d, disambiguate, outset))
                        .toList();
                return new Or(disjuncts);
            }
            case SimpleQueryTree.FreeText ft -> {
                return new FreeText(ft.operator(), ft.value());
            }
            case SimpleQueryTree.PropertyValue pv -> {
                return "rdf:type".equals(pv.property())
                        ? buildTypeField(pv, disambiguate)
                        : buildField(pv, disambiguate, outset);
            }
        }
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv) {
        return buildField(pv, pv.value().string());
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv, String value) {
        return new Field(new Path(pv.propertyPath()), pv.operator(), value);
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
            }
        }

        if (isAccuratePath) {
            return new Field(path, operator, value);
        }

        List<Node> altFields = path.expand(pv.property(), disambiguate, outset)
                .stream()
                .map(p -> newFields(p, operator, value, disambiguate))
                .collect(Collectors.toList());

        if (altFields.size() == 1) {
            return altFields.getFirst();
        }

        return operator == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }

    static Node newFields(Path path, Operator operator, String value, Disambiguate disambiguate) {
        Field f = new Field(path, operator, value);

        if (path.defaultFields.isEmpty()) {
            return f;
        }

        List<Field> fields = new ArrayList<>(List.of(f));

        path.defaultFields.forEach(df -> {
                    Path dfPath = new Path(df.path());
                    if (disambiguate.isObjectProperty(df.path().getLast()) && JsonLd.looksLikeIri(df.value())) {
                        dfPath.appendId();
                    }
                    fields.add(new Field(dfPath, operator, df.value()));
                }
        );

        return new Nested(fields, operator);
    }

    private static Node buildTypeField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate) {
        Set<String> altTypes = "Work".equals(pv.value().string())
                ? disambiguate.workTypes
                : ("Instance".equals(pv.value().string()) ? disambiguate.instanceTypes : Collections.emptySet());

        if (altTypes.isEmpty()) {
            return buildField(pv);
        }

        List<Node> altFields = altTypes.stream()
                .sorted()
                .map(type -> buildField(pv, type))
                .toList();

        return pv.operator() == Operator.NOT_EQUALS ? new And(altFields) : new Or(altFields);
    }
}

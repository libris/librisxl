package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class QueryTree {
    public sealed interface Node permits And, Or, Field, FreeText {
    }

    public record And(List<Node> conjuncts) implements Node {
    }

    public record Or(List<Node> disjuncts) implements Node {
    }

    public record Field(Path path, Operator operator, String value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
    }

    public Node tree;

    public QueryTree(SimpleQueryTree sqt, Disambiguate disambiguate) {
        this.tree = sqtToQt(sqt.tree, disambiguate);
    }

    private static Node sqtToQt(SimpleQueryTree.Node sqt, Disambiguate disambiguate) {
        /*
        If any given property path consists of more than one key (separated by period), assume that the user knows what
        she's doing and interpret each property as is, i.e. don't apply any heuristic for finding alternative paths.
        (See buildField method.)
         */
        if (anyCompositePath(sqt)) {
            return sqtToQt(sqt);
        }

        Disambiguate.OutsetType outset = decideOutset(sqt, disambiguate);
        return sqtToQt(sqt, disambiguate, outset);
    }

    private static Node sqtToQt(SimpleQueryTree.Node sqtNode) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .map(QueryTree::sqtToQt)
                        .toList();
                return new And(conjuncts);
            }
            case SimpleQueryTree.Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(QueryTree::sqtToQt)
                        .toList();
                return new Or(disjuncts);
            }
            case SimpleQueryTree.FreeText ft -> {
                return new FreeText(ft.operator(), ft.value());
            }
            case SimpleQueryTree.PropertyValue pv -> {
                return buildField(pv);
            }
        }
    }

    private static Disambiguate.OutsetType decideOutset(SimpleQueryTree.Node sqtNode, Disambiguate disambiguate) {
        Set<String> givenTypes = collectGivenTypes(sqtNode);
        Set<Disambiguate.OutsetType> outset = givenTypes.stream()
                .map(disambiguate::getOutsetType)
                .collect(Collectors.toSet());

        // TODO: Review this (for now default to Resource)
        return outset.size() == 1 ? outset.stream().findFirst().get() : Disambiguate.OutsetType.RESOURCE;
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
                return pv.property() == JsonLd.getTYPE_KEY()
                        ? buildField(pv)
                        : buildField(pv, disambiguate, outset);
            }
        }
    }

    private static Field buildField(SimpleQueryTree.PropertyValue pv) {
        Path path = new Path(pv.property(), pv.propertyPath());
        String value = JsonLd.getID_KEY().equals(pv.propertyPath().getLast())
                ? Disambiguate.expandPrefixed(pv.value())
                : pv.value();
        return new Field(path, pv.operator(), value);
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate, Disambiguate.OutsetType outset) {
        Path path = new Path(pv.property(), pv.propertyPath());
        Operator operator = pv.operator();
        String value = pv.value();

        path.expandChainAxiom(disambiguate);

        String domain = disambiguate.getDomain(pv.property());

        Disambiguate.DomainCategory domainCategory = disambiguate.getDomainCategory(domain);
        if (domainCategory == Disambiguate.DomainCategory.ADMIN_METADATA) {
            path.prependMeta();
        }

        if (disambiguate.isObjectProperty(pv.property()) && !disambiguate.isVocabTerm(pv.property())) {
            /*
             If "vocab term" interpret the value as is, e.g. issuanceType: "Serial" or encodingLevel: "marc:FullLevel".
             Otherwise, when object property, append either @id or _str to the path.
             */
            String expanded = Disambiguate.expandPrefixed(pv.value());
            if (JsonLd.looksLikeIri(expanded)) {
                path.appendId();
                value = expanded;
            } else {
                path.appendUnderscoreStr();
            }
        }

        return switch (outset) {
            case WORK -> {
                switch (domainCategory) {
                    case INSTANCE, EMBODIMENT -> {
                        // The property p appears only on instance, modify path to @reverse.instanceOf.p...
                        path.setWorkToInstancePath();
                        yield new Field(path, operator, value);
                    }
                    case CREATION_SUPER, UNKNOWN -> {
                        // The property p may appear on instance, add alternative path @reverse.instanceOf.p...
                        List<Node> fields = new ArrayList<>();
                        Path copy = path.copy();
                        copy.setWorkToInstancePath();
                        fields.add(new Field(path, operator, value));
                        fields.add(new Field(copy, operator, value));
                        yield operator == Operator.NOT_EQUALS ? new And(fields) : new Or(fields);
                    }
                    default -> {
                        yield new Field(path, operator, value);
                    }
                }
            }
            case INSTANCE -> {
                switch (domainCategory) {
                    case WORK -> {
                        // The property p appears only work, modify path to instanceOf.p...
                        path.setInstanceToWorkPath();
                        yield new Field(path, operator, value);
                    }
                    case CREATION_SUPER, UNKNOWN -> {
                        // The property p may appear on work, add alternative path instanceOf.p...
                        List<Node> fields = new ArrayList<>();
                        Path copy = path.copy();
                        copy.setInstanceToWorkPath();
                        fields.add(new Field(path, operator, value));
                        fields.add(new Field(copy, operator, value));
                        yield operator == Operator.NOT_EQUALS ? new And(fields) : new Or(fields);
                    }
                    default -> {
                        yield new Field(path, operator, value);
                    }
                }
            }
            case RESOURCE -> new Field(path, operator, value);
        };
    }

    public static Set<String> collectGivenTypes(SimpleQueryTree.Node sqt) {
        return collectGivenTypes(sqt, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(SimpleQueryTree.Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                and.conjuncts().forEach(c -> collectGivenTypes(c, types));
            }
            case SimpleQueryTree.Or or -> {
                or.disjuncts().forEach(d -> collectGivenTypes(d, types));
            }
            case SimpleQueryTree.PropertyValue pv -> {
                if (JsonLd.getTYPE_KEY().equals(pv.property())) {
                    types.add(pv.value());
                }
            }
            case SimpleQueryTree.FreeText ignored -> {
                // Nothing to do here
            }
        }

        return types;
    }

    private static boolean anyCompositePath(SimpleQueryTree.Node sqtNode) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                return and.conjuncts().stream().anyMatch(QueryTree::anyCompositePath);
            }
            case SimpleQueryTree.Or or -> {
                return or.disjuncts().stream().anyMatch(QueryTree::anyCompositePath);
            }
            case SimpleQueryTree.PropertyValue pv -> {
                if (pv.propertyPath().size() > 1) {
                    return true;
                }
            }
            case SimpleQueryTree.FreeText ignored -> {
                // Nothing to do here
            }
        }
        return false;
    }
}

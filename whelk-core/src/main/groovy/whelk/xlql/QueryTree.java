package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Set<String> givenProperties = collectGivenProperties(sqt);
        Set<String> givenTypes = givenProperties.contains(JsonLd.getTYPE_KEY()) ? collectGivenTypes(sqt) : Collections.emptySet();
        Set<String> domains = givenProperties.stream()
                .map(disambiguate::getDomain)
                .collect(Collectors.toSet());

        boolean searchInstances = Stream.of(givenTypes, domains)
                .flatMap(Set::stream)
                .map(disambiguate::getDomainGroup)
                .anyMatch(domainGroup ->
                                switch (domainGroup) {
                                    case OTHER -> true;
                                    case INSTANCE_SUPER -> true;
                                    case INSTANCE -> true;
                                    case ADMIN_METADATA -> false;
                                    case WORK -> false;
                                }
                        );
        /*
        If any given property path consists of more than one key (separated by period), assume that the user knows what
        she's doing and interpret each property as is, i.e. don't apply any heuristic for finding alternative paths.
        (See buildField method.)
         */
        boolean exactPaths = anyCompositePath(sqt);

        // TODO: Review heuristic for which entities to search for
//        return sqtToQt(sqt, disambiguate, exactPaths, searchInstances);
        return sqtToQt(sqt, disambiguate, exactPaths, false);
    }

    private static Node sqtToQt(SimpleQueryTree.Node dtNode, Disambiguate disambiguate, boolean exactPaths, boolean searchInstances) {
        switch (dtNode) {
            case SimpleQueryTree.And and -> {
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .map(c -> sqtToQt(c, disambiguate, exactPaths, searchInstances))
                        .toList();
                return new And(conjuncts);
            }
            case SimpleQueryTree.Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(d -> sqtToQt(d, disambiguate, exactPaths, searchInstances))
                        .toList();
                return new Or(disjuncts);
            }
            case SimpleQueryTree.FreeText ft -> {
                return new FreeText(ft.operator(), ft.value());
            }
            case SimpleQueryTree.PropertyValue pv -> {
                return buildField(pv, disambiguate, exactPaths, searchInstances);
            }
        }
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate, boolean exactPath, boolean searchInstances) {
        if (exactPath) {
            Path path = new Path(pv.property(), pv.propertyPath());
            String value = JsonLd.getID_KEY().equals(pv.propertyPath().getLast())
                    ? Disambiguate.expandPrefixed(pv.value())
                    : pv.value();
            return new Field(path, pv.operator(), value);
        }

        Path path = new Path(pv.property(), pv.propertyPath());

        path.expandChainAxiom(disambiguate);

        String domain = disambiguate.getDomain(pv.property());
        Disambiguate.DomainGroup domainGroup = disambiguate.getDomainGroup(domain);
        if (domainGroup == Disambiguate.DomainGroup.ADMIN_METADATA) {
            path.prependMeta();
        }

        List<Node> fields = new ArrayList<>(List.of(new Field(path, pv.operator(), pv.value())));

        if (disambiguate.isObjectProperty(pv.property())) {
            String expanded = Disambiguate.expandPrefixed(pv.value());
            /*
            Add ._str or .@id as alternative paths but keep the "normal" path since sometimes the value of ObjectProperty
            is a string, e.g. issuanceType: "Serial" or encodingLevel: "marc:FullLevel".
            (Can we skip either path with better disambiguation?)
             */
            if (JsonLd.looksLikeIri(expanded)) {
                Path copy = path.copy();
                copy.appendId();
                fields.add(new Field(copy, pv.operator(), expanded));
            } else {
                Path copy = path.copy();
                copy.appendUnderscoreStr();
                fields.add(new Field(copy, pv.operator(), pv.value()));
            }
        }

        if (searchInstances) {
            fields = fields.stream()
                    .flatMap(f -> collectAltFields((Field) f, domainGroup).stream())
                    .toList();
        }

        if (fields.size() == 1) {
            return fields.get(0);
        }

        return pv.operator() == Operator.NOT_EQUALS ? new And(fields) : new Or(fields);
    }

    private static List<Node> collectAltFields(Field f, Disambiguate.DomainGroup domainGroup) {
        List<Node> fields = new ArrayList<>();
        fields.add(f);
        Path copy = f.path().copy();

        switch (domainGroup) {
            case INSTANCE -> {
                copy.setWorkToInstancePath();
                fields.add(new Field(copy, f.operator(), f.value()));
            }
            case WORK -> {
                copy.setInstanceToWorkPath();
                fields.add(new Field(copy, f.operator(), f.value()));
            }
            default -> {
                copy.setInstanceToWorkPath();
                fields.add(new Field(copy, f.operator(), f.value()));
                Path copy2 = f.path().copy();
                copy2.setWorkToInstancePath();
                fields.add(new Field(copy2, f.operator(), f.value()));
            }
        }

        return fields;
    }

    public static Set<String> collectGivenProperties(SimpleQueryTree.Node sqt) {
        return collectGivenProperties(sqt, new HashSet<>());
    }

    private static Set<String> collectGivenProperties(SimpleQueryTree.Node sqtNode, Set<String> properties) {
        switch (sqtNode) {
            case SimpleQueryTree.And and -> {
                and.conjuncts().forEach(c -> collectGivenProperties(c, properties));
            }
            case SimpleQueryTree.Or or -> {
                or.disjuncts().forEach(d -> collectGivenProperties(d, properties));
            }
            case SimpleQueryTree.PropertyValue pv -> {
                properties.add(pv.property());
            }
            case SimpleQueryTree.FreeText ignored -> {
                // Nothing to do here
            }
        }

        return properties;
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

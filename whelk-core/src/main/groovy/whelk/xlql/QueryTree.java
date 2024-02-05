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

    public QueryTree(SimpleQueryTree dt, Disambiguate disambiguate) {
        this.tree = dtToQt(dt.tree, disambiguate);
    }

    private static Node dtToQt(SimpleQueryTree.Node dtNode, Disambiguate disambiguate) {
        Set<String> givenProperties = collectGivenProperties(dtNode);
        Set<String> givenTypes = givenProperties.contains(JsonLd.getTYPE_KEY()) ? collectGivenTypes(dtNode) : Collections.emptySet();
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

        // TODO: Review heuristic for which entities to search for
//        return dtToQt(dtNode, disambiguate, searchInstances);
        return dtToQt(dtNode, disambiguate, false);
    }

    private static Node dtToQt(SimpleQueryTree.Node dtNode, Disambiguate disambiguate, boolean searchInstances) {
        switch (dtNode) {
            case SimpleQueryTree.And and -> {
                List<Node> conjuncts = and.conjuncts()
                        .stream()
                        .map(c -> dtToQt(c, disambiguate, searchInstances))
                        .toList();
                return new And(conjuncts);
            }
            case SimpleQueryTree.Or or -> {
                List<Node> disjuncts = or.disjuncts()
                        .stream()
                        .map(d -> dtToQt(d, disambiguate, searchInstances))
                        .toList();
                return new Or(disjuncts);
            }
            case SimpleQueryTree.FreeText ft -> {
                return new FreeText(ft.operator(), ft.value());
            }
            case SimpleQueryTree.PropertyValue pv -> {
                return buildField(pv, disambiguate, searchInstances);
            }
        }
    }

    private static Node buildField(SimpleQueryTree.PropertyValue pv, Disambiguate disambiguate, boolean searchInstances) {
        Path path = new Path(pv.property());

        path.expandChainAxiom(disambiguate);

        String domain = disambiguate.getDomain(pv.property());
        Disambiguate.DomainGroup domainGroup = disambiguate.getDomainGroup(domain);
        if (domainGroup == Disambiguate.DomainGroup.ADMIN_METADATA) {
            path.prependMeta();
        }

        List<Field> fields = new ArrayList<>(List.of(new Field(path, pv.operator(), pv.value())));

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
                    .flatMap(f -> collectAltFields(f, domainGroup).stream())
                    .toList();
        }

        if (fields.size() == 1) {
            return fields.get(0);
        }

        return pv.operator() == Operator.NOT_EQUALS ? new And(fields) : new Or(fields);
    }

    private static List<Field> collectAltFields(Field f, Disambiguate.DomainGroup domainGroup) {
        List<Field> fields = new ArrayList<>();
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

    public static Set<String> collectGivenProperties(SimpleQueryTree.Node dtNode) {
        return collectGivenProperties(dtNode, new HashSet<>());
    }

    private static Set<String> collectGivenProperties(SimpleQueryTree.Node dtNode, Set<String> properties) {
        switch (dtNode) {
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

    public static Set<String> collectGivenTypes(SimpleQueryTree.Node dtNode) {
        return collectGivenTypes(dtNode, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(SimpleQueryTree.Node dtNode, Set<String> types) {
        switch (dtNode) {
            case SimpleQueryTree.And and -> {
                and.conjuncts().forEach(c -> collectGivenProperties(c, types));
            }
            case SimpleQueryTree.Or or -> {
                or.disjuncts().forEach(d -> collectGivenProperties(d, types));
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

    private static boolean overlap(Collection<String> a, Collection<String> b) {
        return a.stream().anyMatch(x -> b.contains(x));
    }

    private static Set<String> intersection(Collection<String> a, Collection<String> b) {
        return a.stream().filter(x -> b.contains(x)).collect(Collectors.toSet());
    }
}

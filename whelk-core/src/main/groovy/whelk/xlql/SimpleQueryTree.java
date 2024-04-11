package whelk.xlql;

import whelk.JsonLd;
import whelk.exception.InvalidQueryException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SimpleQueryTree {
    public sealed interface Node permits And, Or, PropertyValue, FreeText {
    }

    public record And(List<Node> conjuncts) implements Node {
    }

    public record Or(List<Node> disjuncts) implements Node {
    }

    public record PropertyValue(String property, List<String> propertyPath, Operator operator,
                                String value) implements Node {
    }

    public record FreeText(Operator operator, String value) implements Node {
    }

    public Node tree;

    private List<PropertyValue> topPvNodes;

    private String freeTextPart;

    public SimpleQueryTree(FlattenedAst ast, Disambiguate disambiguate) throws InvalidQueryException {
        this.tree = buildTree(ast.tree, disambiguate);
    }

    public SimpleQueryTree(SimpleQueryTree.Node tree) {
        this.tree = tree;
    }

    public static Node andExtend(Node tree, Node node) {
        var conjuncts = switch (tree) {
            case And and -> {
                var copy = new ArrayList<>(and.conjuncts());
                if (!copy.contains(node)) {
                    copy.add(node);
                }
                yield copy;
            }
            default -> tree.equals(node) ? List.of(tree) : List.of(tree, node);
        };

        return conjuncts.size() == 1 ? conjuncts.getFirst() : new And(conjuncts);
    }

    private static Node buildTree(FlattenedAst.Node ast, Disambiguate disambiguate) throws InvalidQueryException {
        switch (ast) {
            case FlattenedAst.And and -> {
                List<Node> conjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : and.operands()) {
                    conjuncts.add(buildTree(o, disambiguate));
                }
                return new And(conjuncts);
            }
            case FlattenedAst.Or or -> {
                List<Node> disjuncts = new ArrayList<>();
                for (FlattenedAst.Node o : or.operands()) {
                    disjuncts.add(buildTree(o, disambiguate));
                }
                return new Or(disjuncts);
            }
            case FlattenedAst.Not not -> {
                return new FreeText(Operator.NOT_EQUALS, not.value());
            }
            case FlattenedAst.Leaf l -> {
                return new FreeText(Operator.EQUALS, l.value());
            }
            case FlattenedAst.Code c -> {
                String property = null;
                String value = c.value();
                List<String> propertyPath = new ArrayList<>();

                for (String part : c.code().split("\\.")) {
                    Optional<String> mappedProperty = disambiguate.mapToProperty(part);
                    if (mappedProperty.isPresent()) {
                        property = mappedProperty.get();
                        propertyPath.add(property);
                    } else if (Disambiguate.isLdKey(part) || JsonLd.SEARCH_KEY.equals(part)) {
                        propertyPath.add(part);
                    } else {
                        throw new InvalidQueryException("Unrecognized property alias: " + part);
                    }
                }

                if ("rdf:type".equals(property)) {
                    Optional<String> mappedType = disambiguate.mapToKbvClass(value);
                    if (mappedType.isPresent()) {
                        value = mappedType.get();
                    } else {
                        throw new InvalidQueryException("Unrecognized type: " + value);
                    }
                }

                if (disambiguate.isVocabTerm(property)) {
                    Optional<String> mappedEnum = disambiguate.mapToEnum(value);
                    if (mappedEnum.isPresent()) {
                        value = mappedEnum.get();
                    } else {
                        throw new InvalidQueryException("Invalid value " + value + " for property " + property);
                    }
                }

                // Expand and encode URIs, e.g. sao:Hästar -> https://id.kb.se/term/sao/H%C3%A4star
                if (disambiguate.isObjectProperty(property)) {
                    String expanded = Disambiguate.expandPrefixed(value);
                    if (JsonLd.looksLikeIri(expanded)) {
                        value = URLEncoder.encode(value, StandardCharsets.UTF_8)
                                .replaceAll("\\+", "%20")
                                .replaceAll("%2F", "/")
                                .replaceAll("%3A", ":");
                    }
                }

                return new PropertyValue(property, propertyPath, c.operator(), value);
            }
        }
    }

    public static Node excludeFromTree(Node nodeToExclude, Node tree) {
        if (nodeToExclude.equals(tree)) {
            return null;
        }
        switch (tree) {
            case And and -> {
                List<Node> andClause = and.conjuncts()
                        .stream()
                        .map(c -> excludeFromTree(nodeToExclude, c))
                        .filter(Objects::nonNull)
                        .toList();
                return andClause.size() > 1 ? new And(andClause) : andClause.getFirst();
            }
            case Or or -> {
                List<Node> orClause = or.disjuncts()
                        .stream()
                        .map(d -> excludeFromTree(nodeToExclude, d))
                        .filter(Objects::nonNull)
                        .toList();
                return orClause.size() > 1 ? new Or(orClause) : orClause.getFirst();
            }
            case FreeText ignored -> {
                return tree;
            }
            case PropertyValue ignored -> {
                return tree;
            }
        }
    }

    public Set<String> collectGivenTypes() {
        return collectGivenTypes(tree, new HashSet<>());
    }

    private static Set<String> collectGivenTypes(SimpleQueryTree.Node sqtNode, Set<String> types) {
        switch (sqtNode) {
            case And and -> and.conjuncts().forEach(c -> collectGivenTypes(c, types));
            case Or or -> or.disjuncts().forEach(d -> collectGivenTypes(d, types));
            case PropertyValue pv -> {
                if (List.of("rdf:type").equals(pv.propertyPath())) {
                    types.add(pv.value());
                }
            }
            case FreeText ignored -> {
                // Nothing to do here
            }
        }

        return types;
    }

    public List<PropertyValue> getTopLevelPvNodes() {
        if (topPvNodes == null) {
            topPvNodes = switch (this.tree) {
                case And and -> and.conjuncts()
                        .stream()
                        .filter(node -> node instanceof PropertyValue)
                        .map(PropertyValue.class::cast)
                        .toList();
                case Or ignored -> Collections.emptyList(); //TODO
                case PropertyValue pv -> List.of(pv);
                case FreeText ignored -> Collections.emptyList();
            };
        }

        return topPvNodes;
    }

    public static PropertyValue pvEquals(String property, String value) {
        return new PropertyValue(property, List.of(property), Operator.EQUALS, value);
    }

    public Optional<String> getFreeTextPart() {
        if (freeTextPart == null) {
            switch (tree) {
                case And and -> and.conjuncts()
                        .stream()
                        .filter(c -> c instanceof FreeText)
                        .map(FreeText.class::cast)
                        .filter(ft -> ft.operator() == Operator.EQUALS)
                        .map(FreeText::value)
                        .findFirst()
                        .ifPresent(ft -> freeTextPart = ft);
                case SimpleQueryTree.FreeText ft -> {
                    if (ft.operator() == Operator.EQUALS) {
                        freeTextPart = ft.value();
                    }
                }
                default -> {}
            }
        }

        return Optional.ofNullable(freeTextPart);
    }
}

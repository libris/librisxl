package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;

public record PropertyValue(Property property, Operator operator, Value value) implements Node {
    PropertyValue(String property, Operator operator, Value value) {
        this(new Property(property), operator, value);
    }

    @Override
    public Map<String, Object> toEs() {
        throw new UnsupportedOperationException("Expand before converting to ES");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("property", property.definition());
//        property.getAlias(qt.getOutsetType()).ifPresent(a -> m.put("alias", a));
        m.put(operator.termKey, value.description());
        m.put("up", qt.makeUpLink(this, nonQueryParams));

        return m;
    }

    @Override
    public Node expand(Disambiguate disambiguate, Collection<String> rulingTypes, Function<Collection<String>, Collection<String>> getBoostFields) {
        return property.isRdfType()
                ? buildTypeNode(value, disambiguate).insertOperator(operator)
                : property.expand(disambiguate, rulingTypes)
                .insertOperator(operator)
                .insertValue(value);
    }

    public Node expand(Disambiguate disambiguate) {
        return expand(disambiguate, List.of(), Function.identity());
    }

    @Override
    public String toString(boolean topLevel) {
        return asString();
    }

    private String asString() {
        String p = quoteIfPhraseOrContainsSpecialSymbol(property.canonicalForm());
        String v = quoteIfPhraseOrContainsSpecialSymbol(value.canonicalForm());
        return operator.format(p, v);
    }

    @Override
    public boolean isTypeNode() {
        return property.isRdfType() && operator.equals(Operator.EQUALS);
    }

    public PropertyValue toOrEquals() {
        return switch (operator) {
            case GREATER_THAN ->
                    new PropertyValue(property, Operator.GREATER_THAN_OR_EQUALS, value.increment());
            case LESS_THAN -> new PropertyValue(property, Operator.LESS_THAN_OR_EQUALS, value.decrement());
            default -> this;
        };
    }

    private static Node buildTypeNode(Value type, Disambiguate disambiguate) {
        Set<String> subtypes = disambiguate.getSubclasses(type.string());

        if (subtypes.isEmpty()) {
            return new PathValue(JsonLd.TYPE_KEY, null, type);
        }

        List<Node> altFields = Stream.concat(Stream.of(type.string()), subtypes.stream())
                .sorted()
                .map(t -> (Node) new PathValue(JsonLd.TYPE_KEY, null, new VocabTerm(t)))
                .toList();

        return new Or(altFields);
    }
}

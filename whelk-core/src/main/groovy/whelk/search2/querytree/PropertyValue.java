package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.OutsetType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static whelk.search2.Disambiguate.RDF_TYPE;
import static whelk.search2.QueryUtil.quoteIfPhraseOrContainsSpecialSymbol;

public record PropertyValue(Property property, Operator operator, Value value) implements Node {
    PropertyValue(String property, Operator operator, Value value) {
        this(new Property(property), operator, value);
    }

    @Override
    public Map<String, Object> toEs(List<String> boostedFields) {
        throw new UnsupportedOperationException("Expand before converting to ES");
    }

    @Override
    public Map<String, Object> toSearchMapping(QueryTree qt, Map<String, String> nonQueryParams) {
        Map<String, Object> m = new LinkedHashMap<>();

        m.put("property", property.definition());
        property.getAlias(qt.getOutsetType()).ifPresent(a -> m.put("alias", a));
        m.put(operator.termKey, value.description());
        m.put("up", qt.makeUpLink(this, nonQueryParams));

        return m;
    }

    @Override
    public Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        return property.isRdfType()
                ? buildTypeNode(value, disambiguate).insertOperator(operator)
                : property.expand(disambiguate, outsetType)
                .insertOperator(operator)
                .insertValue(value);
    }

    public Node expand(Disambiguate disambiguate) {
        return expand(disambiguate, OutsetType.RESOURCE);
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

    public PropertyValue toOrEquals() {
        return switch (operator) {
            case GREATER_THAN ->
                    new PropertyValue(property, Operator.GREATER_THAN_OR_EQUALS, value.increment());
            case LESS_THAN -> new PropertyValue(property, Operator.LESS_THAN_OR_EQUALS, value.decrement());
            default -> this;
        };
    }

    private static Node buildTypeNode(Value value, Disambiguate disambiguate) {
        Set<String> altTypes = "Work".equals(value.string())
                ? disambiguate.workTypes
                : ("Instance".equals(value.string()) ? disambiguate.instanceTypes : Collections.emptySet());

        if (altTypes.isEmpty()) {
            return new PathValue(JsonLd.TYPE_KEY, null, value);
        }

        List<Node> altFields = altTypes.stream()
                .sorted()
                .map(type -> (Node) new PathValue(JsonLd.TYPE_KEY, null, new VocabTerm(type)))
                .toList();

        return new Or(altFields);
    }
}

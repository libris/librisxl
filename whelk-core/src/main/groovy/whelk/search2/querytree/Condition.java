package whelk.search2.querytree;

import whelk.search2.Operator;

import java.util.LinkedHashMap;
import java.util.Map;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.hash;
import static whelk.search2.Operator.EQUALS;
import static whelk.search2.Operator.LIKE;
import static whelk.search2.QueryUtil.parenthesize;

public non-sealed class Condition implements Node {
    private final Selector selector;
    private final Operator operator;
    private final Value value;

    public Condition(Selector selector, Operator operator, Value value) {
        this.selector = selector;
        this.operator = operator;
        this.value = value;
    }

    public Condition(String key, Operator operator, Value value) {
        this(new Key.RecognizedKey(new Token.Raw(key)), operator, value);
    }

    public Selector selector() {
        return selector;
    }

    public Operator operator() {
        return operator;
    }

    public Value value() {
        return value;
    }

    @Override
    public String toQueryString(boolean topLevel) {
        var k = selector.formattedQueryKey();
        var v = value.isMultiToken() ? parenthesize(value.queryForm()) : value.queryForm();
        return operator.format(k, v);
    }

    @Override
    public Node getInverse() {
        return operator.isRange() ? withOperator(operator.getInverse()) : new Not(this);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public String toString() {
        return toQueryString(true);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Condition other && hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        return hash(selector, operator, value);
    }

    public Condition withSelector(Selector s) {
        return new Condition(s, operator, value);
    }

    public Condition withOperator(Operator op) {
        return new Condition(selector, op, value);
    }

    public Condition withValue(Value v) {
        return new Condition(selector, operator, v);
    }

    public boolean isTypeNode() {
        return selector instanceof Property.RdfType && operator.equals(EQUALS) && value instanceof VocabTerm;
    }

    public Type asTypeNode() {
        return new Type((Property.RdfType) selector, (VocabTerm) value);
    }
}

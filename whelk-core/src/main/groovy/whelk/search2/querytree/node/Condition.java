package whelk.search2.querytree.node;

import whelk.JsonLd;
import whelk.search2.Operator;
import whelk.search2.querytree.value.FreeText;
import whelk.search2.querytree.selector.Key;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.selector.Selector;
import whelk.search2.querytree.value.Token;
import whelk.search2.querytree.value.Value;
import whelk.search2.querytree.value.VocabTerm;

import java.util.Objects;

import static java.util.Objects.hash;
import static whelk.search2.Operator.EQUALS;

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
    public Node getInverse() {
        return operator.isRange() ? withOperator(operator.getInverse()) : new Not(this);
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Condition other
                && Objects.equals(selector, other.selector())
                && Objects.equals(operator, other.operator())
                && Objects.equals(value, other.value());
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

    public boolean isTextQuery() {
        return selector instanceof Property.TextQuery;
    }

    public boolean isAnyQuery() {
        return selector instanceof Property.AnyQuery;
    }

    public boolean isTypeNode() {
        return selector instanceof Property.RdfType && operator.equals(EQUALS) && value instanceof VocabTerm;
    }

    public Type asTypeNode() {
        return new Type((Property.RdfType) selector, (VocabTerm) value);
    }

    public FreeText freeTextValue() {
        if (value instanceof FreeText ft) {
            return ft;
        }
        throw new IllegalStateException("Value is not FreeText");
    }

    public final static class Type extends Condition {
        private final Property.RdfType rdfTypeProperty;
        private final String type;

        public Type(Property.RdfType rdfTypeProperty, VocabTerm value) {
            super(rdfTypeProperty, Operator.EQUALS, value);
            this.rdfTypeProperty = rdfTypeProperty;
            this.type = value.jsonForm();
        }

        public Type(String raw, JsonLd jsonld) {
            this(new Property.RdfType(jsonld), new VocabTerm(raw, jsonld.vocabIndex.get(raw)));
        }

        public Property.RdfType rdfTypeProperty() {
            return rdfTypeProperty;
        }

        public String type() {
            return type;
        }
    }
}

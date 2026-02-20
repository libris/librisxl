package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Operator;

public final class Type extends Condition {
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

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return implies(node, n -> n instanceof Type t && implies(this, t, jsonLd));
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return new RdfSubjectType(this);
    }

    public Property.RdfType rdfTypeProperty() {
        return rdfTypeProperty;
    }

    public String type() {
        return type;
    }

    private static boolean implies(Type a, Type b, JsonLd jsonLd) {
        return jsonLd.isSubClassOf(a.type(), b.type());
    }
}

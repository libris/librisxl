package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Operator;

import java.util.List;
import java.util.Optional;

import static whelk.JsonLd.Rdfs.RDF_TYPE;

public final class Type extends PathValue {
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
    public Optional<Node> subjectTypesNode() {
        return Optional.of(this);
    }

    @Override
    public List<String> subjectTypesList() {
        return List.of(type);
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

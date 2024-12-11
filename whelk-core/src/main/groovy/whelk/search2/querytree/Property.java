package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static whelk.search2.Disambiguate.Rdfs.RDF_TYPE;

public class Property {
    private final String name;
    private Map<String, Object> definition;
    private boolean isVocabTerm;
    private boolean isType;

    public Property(String name) {
        this.name = name;
        this.definition = Collections.emptyMap();
    }

    public Property(String name, Map<String, Object> definition) {
        this.name = name;
        this.definition = definition;
    }

    public Property(String name, Disambiguate disambiguate) {
        this.name = name;
        setVars(disambiguate);
    }

    public String name() {
        return name;
    }

    public Map<String, Object> definition() {
        return definition;
    }

    public String canonicalForm() {
        return (String) definition.getOrDefault("librisQueryCode", name);
    }

    public boolean isRdfType() {
        return RDF_TYPE.equals(name);
    }

    public boolean isType() {
        return isType;
    }

    public boolean isObjectProperty() {
        return Disambiguate.isObjectProperty(definition);
    }

    public boolean hasVocabValue() {
        return isVocabTerm || isType;
    }

    public boolean isVocabTerm() {
        return isVocabTerm;
    }

    public Node expand(Disambiguate disambiguate, Collection<String> types) {
        if (definition.isEmpty()) {
            setVars(disambiguate);
        }

        var expanded = isShortHand()
                ? disambiguate.expandChainAxiom(this)
                : new PathValue(List.of(this), null, null);

        if (!isType) {
            List<String> domain = disambiguate.getDomain(name);
            if (!domain.isEmpty()
                    && domain.stream()
                    .filter(d -> disambiguate.isSubclassOf(d, "Record"))
                    .count() == domain.size()
            ) {
                // The property only appears on Record
                expanded = prependMetaKey(expanded);
            } else {
                expanded = getAlternativePaths(expanded, disambiguate, types);
            }
        }

        return expanded;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Property && ((Property) o).name().equals(name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }

    private static Node prependMetaKey(Node n) {
        return n.modifyAllPathValue(path -> path.prepend(JsonLd.RECORD_KEY));
    }

    private boolean isShortHand() {
        // TODO: All short forms should be marked with :category :shortHand?
        return definition.containsKey("propertyChainAxiom");
    }

    private Node getAlternativePaths(Node n, Disambiguate disambiguate, Collection<String> types) {
        List<String> applicableIntegralRelations = types.stream()
                .map(disambiguate::getIntegralRelationsForType)
                .flatMap(List::stream)
                .distinct()
                .toList();

        List<Node> altPaths = applicableIntegralRelations.stream()
                .filter(ir -> disambiguate.getRange(ir).stream().anyMatch(type -> mayAppearOnType(type, disambiguate)))
                .map(ir -> n.modifyAllPathValue(pathValue -> pathValue.prepend(ir)))
                .toList();

        if (!altPaths.isEmpty()) {
            return types.stream().anyMatch(t -> mayAppearOnType(t, disambiguate))
                    ? new Or(Stream.concat(Stream.of(n), altPaths.stream()).toList())
                    : (altPaths.size() == 1 ? altPaths.getFirst() : new Or(altPaths));
        }

        return n;
    }

    private boolean mayAppearOnType(String type, Disambiguate disambiguate) {
        List<String> domain = disambiguate.getDomain(name);
        return domain.isEmpty()
                || domain.stream().anyMatch(d -> disambiguate.isSubclassOf(d, type) || disambiguate.isSubclassOf(type, d));
    }

    private void setVars(Disambiguate disambiguate) {
        this.definition = disambiguate.getDefinition(name);
        this.isVocabTerm = disambiguate.isVocabTerm(name);
        this.isType = disambiguate.isType(name);
    }
}
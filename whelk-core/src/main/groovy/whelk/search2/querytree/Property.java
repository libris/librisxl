package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;
import whelk.search2.OutsetType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static whelk.search2.Disambiguate.RDF_TYPE;

public class Property {
    public enum DomainCategory {
        ADMIN_METADATA,
        WORK,
        INSTANCE,
        CREATION_SUPER,
        EMBODIMENT,
        UNKNOWN,
        OTHER
    }

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
        return Disambiguate.RDF_TYPE.equals(name);
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

    public Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        if (definition.isEmpty()) {
            setVars(disambiguate);
        }

        var expanded = isShortHand()
                ? disambiguate.expandChainAxiom(this)
                : new PathValue(List.of(this), null, null);

        if (!isType) {
            var domainCategory = disambiguate.getDomainCategory(name);
            if (domainCategory == DomainCategory.ADMIN_METADATA) {
                expanded = prependMetaKey(expanded);
            }
            expanded = setAlternativePaths(expanded, domainCategory, outsetType);
        }

        return expanded;
    }

    public Optional<String> getAlias(OutsetType outsetType) {
        var alias = switch (outsetType) {
            case INSTANCE -> switch (name) {
                case RDF_TYPE -> "instanceType";
                case "instanceOfType" -> "workType";
                default -> null;
            };
            case WORK, RESOURCE -> switch (name) {
                case RDF_TYPE -> "workType";
                case "hasInstanceType" -> "instanceType";
                default -> null;
            };
        };

        return Optional.ofNullable(alias);
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

    private Node setAlternativePaths(Node n, DomainCategory domainCategory, OutsetType outsetType) {
        return switch (outsetType) {
            case WORK -> switch (domainCategory) {
                // The property p appears only on instance, modify path to @reverse.instanceOf.p...
                case INSTANCE, EMBODIMENT -> setWorkToInstancePath(n);
                // The property p may appear on instance, add alternative path @reverse.instanceOf.p...
                case CREATION_SUPER, UNKNOWN -> new Or(List.of(n, setWorkToInstancePath(n)));
                default -> n;
            };
            case INSTANCE -> switch (domainCategory) {
                // The property p appears only work, modify path to instanceOf.p...
                case WORK -> setWorkToInstancePath(n);
                // The property p may appear on work, add alternative path instanceOf.p...
                case CREATION_SUPER, UNKNOWN -> new Or(List.of(n, setInstanceToWorkPath(n)));
                default -> n;
            };
            case RESOURCE -> n;
        };
    }

    private static Node setWorkToInstancePath(Node n) {
        return n.modifyAllPathValue(pathValue -> pathValue.prepend(List.of(JsonLd.REVERSE_KEY, JsonLd.WORK_KEY)));
    }

    private static Node setInstanceToWorkPath(Node n) {
        return n.modifyAllPathValue(pathValue -> pathValue.prepend(JsonLd.WORK_KEY));
    }

    private void setVars(Disambiguate disambiguate) {
        this.definition = disambiguate.getDefinition(name);
        this.isVocabTerm = disambiguate.isVocabTerm(name);
        this.isType = disambiguate.isType(name);
    }
}
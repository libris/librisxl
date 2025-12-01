package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;
import whelk.search2.QueryUtil;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.Owl.DATATYPE_PROPERTY;
import static whelk.JsonLd.Owl.HAS_VALUE;
import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.OBJECT_PROPERTY;
import static whelk.JsonLd.Owl.ON_PROPERTY;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;
import static whelk.JsonLd.Owl.RESTRICTION;
import static whelk.JsonLd.Rdfs.DOMAIN;
import static whelk.JsonLd.Rdfs.RANGE;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.Rdfs.SUBCLASS_OF;
import static whelk.JsonLd.Rdfs.SUB_PROPERTY_OF;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.JsonLd.isLink;


public non-sealed class Property implements Subpath {
    protected String name;
    protected Key.RecognizedKey queryKey;
    protected String indexKey;

    protected Map<String, Object> definition;
    protected List<String> domain;
    protected List<String> range;
    protected String inverseOf;
    protected boolean isVocabTerm;
    protected List<Property> propertyChain;

    protected Property superProperty;
    protected List<Restrictions.OnProperty> objectOnPropertyRestrictions;

    private static final String LIBRIS_SEARCH_NS = "librissearch:";

    public Property(String name, JsonLd jsonLd) {
        this(jsonLd.vocabIndex.get(name), jsonLd);
        this.name = name;
        this.isVocabTerm = jsonLd.isVocabTerm(name);
    }

    protected Property(Map<String, Object> definition, JsonLd jsonLd) {
        this.definition = definition;
        this.domain = getDomain(jsonLd);
        this.range = getRange(jsonLd);
        this.inverseOf = getInverseOf(jsonLd);
        this.propertyChain = getPropertyChain(jsonLd);
        this.indexKey = (String) definition.get("ls:indexKey"); // FIXME: This shouldn't have a different prefix (ls: vs librissearch:)
    }

    // For test only
    public Property(String name, Map<String, Object> definition, String queryKey) {
        this.name = name;
        this.definition = definition;
        this.queryKey = new Key.RecognizedKey(queryKey);
    }

    private Property(String name, JsonLd jsonLd, Key.RecognizedKey queryKey) {
        this(name, jsonLd);
        this.queryKey = queryKey;
    }

    protected Property() {
    }

    public static Property getProperty(String propertyKey, JsonLd jsonLd) {
        return getProperty(propertyKey, jsonLd, null);
    }

    public static Property getProperty(String propertyKey, JsonLd jsonLd, Key.RecognizedKey queryKey) {
        if (jsonLd.vocabIndex.containsKey(LIBRIS_SEARCH_NS + propertyKey)) {
            // FIXME: This is only temporary to avoid having to include the prefix for terms in the libris search namespace
            return getProperty(LIBRIS_SEARCH_NS + propertyKey, jsonLd, new Key.RecognizedKey(propertyKey));
        }
        var propDef = jsonLd.vocabIndex.get(propertyKey);
        if (propDef == null) {
            throw new IllegalArgumentException("No such property: " + propertyKey);
        }
        if (Restrictions.isNarrowingProperty(propertyKey)) {
            return new NarrowedRestrictedProperty(propertyKey, jsonLd);
        }
        return RDF_TYPE.equals(propertyKey)
                ? new Property.RdfType(jsonLd, queryKey)
                : new Property(propertyKey, jsonLd, queryKey);
    }

    protected Property getSuperProperty(JsonLd jsonLd) {
        return getProperty(getSuperKey(definition, jsonLd), jsonLd);
    }

    public boolean isRestrictedSubProperty() {
        return definition.containsKey(SUB_PROPERTY_OF) && !objectOnPropertyRestrictions().isEmpty();
    }

    public boolean hasIndexKey() {
        return indexKey != null;
    }

    public void loadRestrictions(Disambiguate disambiguate) {
        propertyChain.forEach(p -> p.loadRestrictions(disambiguate));
        List<Restrictions.OnProperty> restrictions = new ArrayList<>();
        getObjectHasValueRestrictions(definition).forEach((onProperty, hasValues) ->
                hasValues.forEach(hv -> {
                    Property p = disambiguate.mapPropertyKey(onProperty);
                    Value v = disambiguate.mapValueForProperty(p, hv).orElseThrow();
                    restrictions.add(new Restrictions.HasValue(p, v));
                }));
        this.objectOnPropertyRestrictions = restrictions;
    }

    public String name() {
        return name;
    }

    public Map<String, Object> definition() {
        return definition;
    }

    public List<String> domain() {
        return domain != null ? domain : List.of();
    }

    public List<String> range() {
        return range != null ? range : List.of();
    }

    @Override
    public String queryKey() {
        return queryKey != null ? queryKey.value() : name;
    }

    @Override
    public boolean isType() {
        return isRdfType() || (!propertyChain.isEmpty() && propertyChain.getLast().isRdfType());
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public String indexKey() {
        return indexKey != null ? indexKey : name;
    }

    public boolean isRdfType() {
        return name.equals(RDF_TYPE);
    }

    public boolean isVocabTerm() {
        return isVocabTerm;
    }

    public boolean isPlatformTerm() {
        return ((List<?>) asList(definition.get("category"))).stream()
                .anyMatch(c -> Map.of(JsonLd.ID_KEY, "https://id.kb.se/vocab/platform").equals(c));
    }

    public boolean isXsdDate() {
        return range.contains("xsd:dateTime") || range.contains("xsd:date");
    }

    public boolean isObjectProperty() {
        return ((List<?>) asList(definition.get(TYPE_KEY))).stream().anyMatch(OBJECT_PROPERTY::equals);
    }

    public boolean isDatatypeProperty() {
        return ((List<?>) asList(definition.get(TYPE_KEY))).stream().anyMatch(DATATYPE_PROPERTY::equals);
    }

    public boolean hasDomainAdminMetadata(JsonLd jsonLd) {
        return !domain.isEmpty() && domain.stream()
                .filter(d -> jsonLd.isSubClassOf(d, "AdminMetadata"))
                .count() == domain.size();
    }

    public boolean mayAppearOnType(String type, JsonLd jsonLd) {
        return domain.isEmpty() || domain.stream().anyMatch(d -> jsonLd.directDescendants(d, type));
    }

    public boolean appearsOnType(String type, JsonLd jsonLd) {
        // TODO: How strict should this be?
//        return !domain.isEmpty() && domain.stream().anyMatch(d -> jsonLd.isSubClassOf(d, type));
        return !domain.isEmpty() && domain.stream().anyMatch(d -> jsonLd.directDescendants(d, type));
    }

    public boolean indirectlyAppearsOnType(String type, JsonLd jsonLd) {
        return QueryUtil.getIntegralRelationsForType(type, jsonLd).stream()
                .anyMatch(relation -> relation.range().stream().anyMatch(t -> appearsOnType(t, jsonLd)));
    }

    public boolean isInverseOf(Property property) {
        return property.name().equals(inverseOf);
    }

    public List<Property> expand() {
        return isShorthand() ? propertyChain : List.of(this);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Property p && name().equals(p.name());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }

    public List<Restrictions.OnProperty> objectOnPropertyRestrictions() {
        return objectOnPropertyRestrictions != null ? objectOnPropertyRestrictions : List.of();
    }

    public static Map<String, Set<String>> getObjectHasValueRestrictions(Map<String, Object> definition) {
        Map<String, Set<String>> restrictions = new HashMap<>();

        ((List<?>) asList(definition.get(RANGE))).stream()
                .map(Map.class::cast)
                .map(m -> (List<?>) m.get(SUBCLASS_OF))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(Map.class::cast)
                .filter(superClass -> RESTRICTION.equals(superClass.get(TYPE_KEY)))
                .forEach(restriction -> {
                    var onPropertyIri = Optional.ofNullable(restriction.get(ON_PROPERTY)).map(Property::getIri);
                    var hasValueIri = Optional.ofNullable(restriction.get(HAS_VALUE)).map(Property::getIri);
                    // TODO: Accept literal values?
                    if (onPropertyIri.isPresent() && hasValueIri.isPresent()) {
                        restrictions.computeIfAbsent(onPropertyIri.get(), k -> new HashSet<>())
                                .add(hasValueIri.get());
                    }
                });

        return restrictions;
    }

    private List<String> getDomain(JsonLd jsonLd) {
        return findDomainOrRange(DOMAIN, jsonLd);
    }

    private List<String> getRange(JsonLd jsonLd) {
        return findDomainOrRange(RANGE, jsonLd);
    }

    private String getInverseOf(JsonLd jsonLd) {
        return Optional.ofNullable(definition.get(INVERSE_OF))
                .map(Property::getIri)
                .map(jsonLd::toTermKey)
                .filter(jsonLd.vocabIndex::containsKey)
                .orElse(null);
    }

    private List<Property> getPropertyChain(JsonLd jsonLd) {
        if (!isShorthand() || !definition.containsKey(PROPERTY_CHAIN_AXIOM)) {
            return List.of();
        }

        return ((List<?>) definition.get(PROPERTY_CHAIN_AXIOM))
                .stream()
                .map(QueryUtil::castToStringObjectMap)
                .map(prop -> isLink(prop)
                        ? getProperty(jsonLd.toTermKey((String) prop.get(ID_KEY)), jsonLd)
                        : new AnonymousProperty(prop, jsonLd))
                .toList();
    }

    private boolean isShorthand() {
        return ((List<?>) asList(definition.get("category"))).stream()
                .anyMatch(c -> Map.of(JsonLd.ID_KEY, "https://id.kb.se/vocab/shorthand").equals(c));
    }

    private List<String> findDomainOrRange(String domainOrRange, JsonLd jsonLd) {
        return findDomainOrRange(definition, domainOrRange, jsonLd);
    }

    private static List<String> findDomainOrRange(Map<String, Object> definition, String domainOrRange, JsonLd jsonLd) {
        String p = definition.containsKey(domainOrRange) ? domainOrRange : domainOrRange + "Includes";
        List<String> keys = getTermKeys(asList(definition.get(p)), jsonLd).toList();
        return keys.isEmpty() ? inheritFromSuper(definition, domainOrRange, jsonLd) : keys;
    }

    private static List<String> inheritFromSuper(Map<String, Object> definition, String domainOrRange, JsonLd jsonLd) {
        return getSuperKeys(definition, jsonLd)
                .map(jsonLd.vocabIndex::get)
                .filter(Objects::nonNull)
                .flatMap(superDef -> findDomainOrRange(superDef, domainOrRange, jsonLd).stream())
                .toList();
    }

    private static String getSuperKey(Map<String, Object> definition, JsonLd jsonLd) {
        return getSuperKeys(definition, jsonLd).findFirst().orElse(null);
    }

    private static Stream<String> getSuperKeys(Map<String, Object> definition, JsonLd jsonLd) {
        return getTermKeys(asList(definition.get(SUB_PROPERTY_OF)), jsonLd);
    }

    private static Stream<String> getTermKeys(List<?> terms, JsonLd jsonLd) {
        return terms.stream()
                .map(Property::getIri)
                .filter(Objects::nonNull)
                .map(jsonLd::toTermKey);
    }

    private static String getIri(Object o) {
        return (String) ((Map<?, ?>) o).get(ID_KEY);
    }

    public static final class TextQuery extends Property {
        public TextQuery(JsonLd jsonLd) {
            super("textQuery", jsonLd);
        }
    }

    public static final class RdfType extends Property {
        public RdfType(JsonLd jsonLd) {
            this(jsonLd, null);
        }

        public RdfType(JsonLd jsonLd, Key.RecognizedKey key) {
            super(RDF_TYPE, jsonLd, key);
            this.indexKey = TYPE_KEY;
        }
    }

    public static final class NarrowedRestrictedProperty extends Property {
        public NarrowedRestrictedProperty(Property superProperty, String subPropertyKey, JsonLd jsonLd) {
            super(subPropertyKey, jsonLd);
            this.superProperty = superProperty;
        }

        public NarrowedRestrictedProperty(String subPropertyKey, JsonLd jsonLd) {
            super(subPropertyKey, jsonLd);
            this.superProperty = getSuperProperty(jsonLd);
        }

        @Override
        public String queryKey() {
            return superProperty.queryKey();
        }

        @Override
        public Map<String, Object> definition() {
            return superProperty.definition();
        }

        @Override
        public String indexKey() {
            if (hasIndexKey()) {
                return indexKey;
            }
            throw new IllegalStateException();
        }

        @Override
        public boolean isRestrictedSubProperty() {
            return true;
        }
    }

    private static final class AnonymousProperty extends Property {
        public AnonymousProperty(Map<String, Object> definition, JsonLd jsonLd) {
            super(definition, jsonLd);
            if (definition.containsKey(SUB_PROPERTY_OF)) {
                this.superProperty = getSuperProperty(jsonLd);
            }
        }

        @Override
        public String queryKey() {
            if (superProperty != null) {
                return superProperty.queryKey();
            }
            throw new IllegalStateException();
        }

        @Override
        public String indexKey() {
            if (hasIndexKey()) {
                return indexKey;
            }
            if (superProperty != null) {
                return superProperty.indexKey();
            }
            throw new IllegalStateException();
        }

        @Override
        public void loadRestrictions(Disambiguate disambiguate) {
            if (hasNarrowedRange()) {
                /*
                We interpret the range of an anonymous sub-property as a restriction.

                For example

                :isbn owl:propertyChainAxiom (
                    [ rdfs:subPropertyOf :identifiedBy ; rdfs:range :ISBN ]
                    :value ) .

                is interpreted as

                :isbn owl:propertyChainAxiom (
                    [ rdfs:subPropertyOf :identifiedBy ;
                        rdfs:range [ rdfs:subClassOf [ a owl:Restriction ; owl:onProperty rdf:type ; owl:hasValue :ISBN ] ] ]
                    :value ) .
                 */
                RdfType rdfType = (RdfType) disambiguate.mapPropertyKey(RDF_TYPE);
                VocabTerm value = (VocabTerm) disambiguate.mapValueForProperty(rdfType, range.getFirst()).orElseThrow();
                this.objectOnPropertyRestrictions = List.of(new Restrictions.HasValue(rdfType, value));
            } else {
                super.loadRestrictions(disambiguate);
            }
        }

        @Override
        public String toString() {
            if (superProperty != null) {
                return superProperty.toString();
            }
            return "AnonymousProperty";
        }

        private boolean hasNarrowedRange() {
            return range.size() == 1;
        }
    }
}
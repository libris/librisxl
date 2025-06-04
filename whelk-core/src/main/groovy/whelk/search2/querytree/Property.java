package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;

import java.util.*;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.JsonLd.isLink;
import static whelk.JsonLd.Owl.*;
import static whelk.JsonLd.Rdfs.*;


public non-sealed class Property implements Subpath {
    private final String name;
    private final Map<String, Object> definition;
    private Key.RecognizedKey mappedKey;

    private List<String> domain;
    private List<String> range;
    private String inverseOf;

    private boolean isVocabTerm;

    public record Restriction(Property property, Value value) {
    }

    private List<Restriction> restrictions;
    private List<Property> propertyChain;

    public Property(String name, JsonLd jsonLd, Key.RecognizedKey mappedKey) {
        this(name, jsonLd);
        this.mappedKey = mappedKey;
    }

    public Property(String name, JsonLd jsonLd) {
        this.name = name;
        this.definition = jsonLd.vocabIndex.get(name);
        this.domain = getDomain(jsonLd);
        this.range = getRange(jsonLd);
        this.inverseOf = getInverseOf(jsonLd);
        this.propertyChain = getPropertyChain(jsonLd);
        this.restrictions = getOnPropertyRestrictions(jsonLd);
        this.isVocabTerm = jsonLd.isVocabTerm(name);
    }

    // For test only
    public Property(String name, Map<String, Object> definition, String mappedKey) {
        this.name = name;
        this.definition = definition;
        this.mappedKey = new Key.RecognizedKey(mappedKey);
    }

    private Property(Map<String, Object> anonymousPropertyDef, JsonLd jsonLd) {
        this.definition = anonymousPropertyDef;
        this.name = getSuperKey(jsonLd);
        this.domain = getDomain(jsonLd);
        this.range = getRange(jsonLd);
        this.inverseOf = getInverseOf(jsonLd);
        this.propertyChain = getPropertyChain(jsonLd);
        this.restrictions = getAllRangeRestrictions(jsonLd);
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

    public List<Restriction> restrictions() {
        return restrictions != null ? restrictions : List.of();
    }

    @Override
    public Key key() {
        return mappedKey != null ? mappedKey : new Key.RecognizedKey(name);
    }

    @Override
    public boolean isType() {
        return isRdfType() || (!propertyChain.isEmpty() && propertyChain.getLast().isRdfType());
    }

    @Override
    public boolean isValid() {
        return true;
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

    public boolean isObjectProperty() {
        return ((List<?>) asList(definition.get(TYPE_KEY))).stream().anyMatch(OBJECT_PROPERTY::equals);
    }

    public boolean hasDomainAdminMetadata(JsonLd jsonLd) {
        return !domain.isEmpty() && domain.stream()
                .filter(d -> jsonLd.isSubClassOf(d, "AdminMetadata"))
                .count() == domain.size();
    }

    public boolean mayAppearOnType(String type, JsonLd jsonLd) {
        return domain.isEmpty() || domain.stream().anyMatch(d -> directDescendants(d, type, jsonLd));
    }

    public boolean isInverseOf(Property property) {
        return property.name().equals(inverseOf);
    }

    public List<Property> expand() {
        return isShortHand() ? propertyChain : List.of(this);
    }

    public List<Property> getApplicableIntegralRelations(JsonLd jsonLd, Collection<String> types) {
//        TODO
//        List<Property> integralRelations = jsonLd.getCategoryMembers("integral").stream()
//              .map(p -> new Property(p, jsonLd))
//              .toList();
        List<Property> integralRelations = List.of(new Property("hasInstance", jsonLd), new Property("instanceOf", jsonLd));

        if (name.equals(RECORD_KEY) || isRdfType()) {
            return List.of();
        }

        return types.stream()
                .map(t -> getIntegralRelationsForType(t, integralRelations, jsonLd))
                .flatMap(List::stream)
                .distinct()
                .filter(ir -> ir.range().stream().anyMatch(irRangeType -> mayAppearOnType(irRangeType, jsonLd)))
                .filter(prop -> !(isInverseOf(prop) || (isShortHand() && propertyChain.getFirst().isInverseOf(prop))))
                .toList();
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Property p && name.equals(p.name());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    private List<Property> getIntegralRelationsForType(String type, Collection<Property> integralRelations, JsonLd jsonLd) {
        return integralRelations.stream()
                .filter(prop -> prop.domain().stream().anyMatch(d -> directDescendants(d, type, jsonLd)))
                .toList();
    }

    private static boolean directDescendants(String a, String b, JsonLd jsonLd) {
        return jsonLd.isSubClassOf(a, b) || jsonLd.isSubClassOf(b, a);
    }

    private String getSuperKey(JsonLd jsonLd) {
        return getSuperKeys(definition, jsonLd)
                .findFirst()
                .orElseThrow();
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
        if (!isShortHand()) {
            return List.of();
        }

        return ((List<?>) definition.get(PROPERTY_CHAIN_AXIOM))
                .stream()
                .map(QueryUtil::castToStringObjectMap)
                .map(prop -> isLink(prop)
                        ? new Property(jsonLd.toTermKey((String) prop.get(ID_KEY)), jsonLd)
                        : new Property(prop, jsonLd))
                .toList();
    }

    private boolean isShortHand() {
        return ((List<?>) asList(definition.get("category"))).stream()
                .anyMatch(c -> Map.of(JsonLd.ID_KEY, "https://id.kb.se/vocab/shorthand").equals(c));
    }

    private List<Restriction> getAllRangeRestrictions(JsonLd jsonLd) {
        return Stream.concat(getTypeRestriction(jsonLd).stream(), getOnPropertyRestrictions(jsonLd).stream()).toList();
    }

    private List<Restriction> getTypeRestriction(JsonLd jsonLd) {
        return getTermKeys(asList(definition.get(RANGE)), jsonLd)
                .map(type -> new Restriction(new Property(RDF_TYPE, jsonLd), new VocabTerm(type, jsonLd.vocabIndex.get(type))))
                .toList();
    }

    private List<Restriction> getOnPropertyRestrictions(JsonLd jsonLd) {
        return ((List<?>) asList(definition.get(RANGE))).stream()
                .map(Map.class::cast)
                .map(m -> (List<?>) m.get(SUBCLASS_OF))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(Map.class::cast)
                .filter(superClass -> RESTRICTION.equals(superClass.get(TYPE_KEY)))
                .map(superClass -> {
                    var onPropertyIri = Optional.ofNullable(superClass.get(ON_PROPERTY)).map(Property::getIri);
                    var hasValueIri = Optional.ofNullable(superClass.get(HAS_VALUE)).map(Property::getIri);
                    if (onPropertyIri.isPresent() && hasValueIri.isPresent()) {
                        var onPropertyKey = jsonLd.toTermKey(onPropertyIri.get());
                        var hasValueKey = jsonLd.toTermKey(hasValueIri.get());
                        return new Restriction(new Property(onPropertyKey, jsonLd),
                                jsonLd.vocabIndex.containsKey(hasValueKey)
                                        ? new VocabTerm(hasValueKey, jsonLd.vocabIndex.get(hasValueKey))
                                        : new Link(hasValueIri.get()));
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
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

    public static class TextQuery extends Property {
        public TextQuery(JsonLd jsonLd) {
            super("textQuery", jsonLd);
        }
    }
}
package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.QueryUtil;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.Owl.DATATYPE_PROPERTY;
import static whelk.JsonLd.Owl.HAS_VALUE;
import static whelk.JsonLd.Owl.INVERSE_OF;
import static whelk.JsonLd.Owl.OBJECT_PROPERTY;
import static whelk.JsonLd.Owl.ON_PROPERTY;
import static whelk.JsonLd.Owl.PROPERTY_CHAIN_AXIOM;
import static whelk.JsonLd.Owl.RESTRICTION;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.JsonLd.Rdfs.DOMAIN;
import static whelk.JsonLd.Rdfs.RANGE;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.Rdfs.SUBCLASS_OF;
import static whelk.JsonLd.Rdfs.SUB_PROPERTY_OF;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.JsonLd.isLink;

public non-sealed class Property implements Selector {
    protected String name;
    protected Key.RecognizedKey queryKey;
    protected String indexKey;

    protected Map<String, Object> definition;
    protected List<String> domain;
    protected List<String> range;
    protected String inverseOf;
    protected boolean isVocabTerm;

    protected Property superProperty;
    protected List<Restrictions.OnProperty> objectOnPropertyRestrictions;

    private static final String LIBRIS_SEARCH_NS = "librissearch:";

    // TODO: Get substitutions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

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
        this.indexKey = (String) definition.get("ls:indexKey"); // FIXME: This shouldn't have a different prefix (ls: vs librissearch:)
        this.objectOnPropertyRestrictions = getObjectHasValueRestrictions(jsonLd);
    }

    private Property(String name, JsonLd jsonLd, Key.RecognizedKey queryKey) {
        this(name, jsonLd);
        this.queryKey = queryKey;
    }

    protected Property() {
    }

    public static Property getProperty(String propertyKey, JsonLd jsonLd) {
        return buildProperty(propertyKey, jsonLd, null);
    }

    public static Property buildProperty(String propertyKey, JsonLd jsonLd, Key.RecognizedKey queryKey) {
        if (jsonLd.vocabIndex.containsKey(LIBRIS_SEARCH_NS + propertyKey)) {
            // FIXME: This is only temporary to avoid having to include the prefix for terms in the libris search namespace
            return buildProperty(LIBRIS_SEARCH_NS + propertyKey, jsonLd, new Key.RecognizedKey(new Token.Raw(propertyKey)));
        }
        var propDef = jsonLd.vocabIndex.get(propertyKey);
        if (propDef == null) {
            throw new IllegalArgumentException("No such property: " + propertyKey);
        }
        if (isComposite(propDef)) {
            return new CompositeProperty(propertyKey, jsonLd, queryKey);
        }
        if (isShorthand(propDef)) {
            return new ShorthandProperty(propertyKey, jsonLd, queryKey);
        }
        if (Restrictions.isNarrowingProperty(propertyKey)) {
            return new NarrowedRestrictedProperty(propertyKey, jsonLd, queryKey);
        }
        return RDF_TYPE.equals(propertyKey)
                ? new RdfType(jsonLd, queryKey)
                : new Property(propertyKey, jsonLd, queryKey);
    }

    @Override
    public String queryKey() {
        return queryKey != null ? queryKey.queryKey() : name;
    }

    @Override
    public String esField() {
        return indexKey != null ? indexKey : substitutions.getOrDefault(name, name);
    }

    @Override
    public List<Selector> path() {
        return List.of(this);
    }

    @Override
    public Selector expand(JsonLd jsonLd) {
        return hasDomainAdminMetadata(jsonLd)
                ? new Path(List.of(new Property(RECORD_KEY, jsonLd), this))
                : this;
    }

    @Override
    public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return _getAltSelectors(jsonLd, rdfSubjectTypes);
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean isType() {
        return isRdfType();
    }

    @Override
    public boolean isObjectProperty() {
        return ((List<?>) asList(definition.get(TYPE_KEY))).stream().anyMatch(OBJECT_PROPERTY::equals);
    }

    @Override
    public boolean mayAppearOnType(String type, JsonLd jsonLd) {
        return domain.isEmpty() || domain.stream().anyMatch(d -> jsonLd.directDescendants(d, type));
    }

    @Override
    public boolean appearsOnType(String type, JsonLd jsonLd) {
        // TODO: How strict should this be?
//        return !domain.isEmpty() && domain.stream().anyMatch(d -> jsonLd.isSubClassOf(d, type));
        return !domain.isEmpty() && domain.stream().anyMatch(d -> jsonLd.directDescendants(d, type));
    }

    @Override
    public boolean indirectlyAppearsOnType(String type, JsonLd jsonLd) {
        return QueryUtil.getIntegralRelationsForType(type, jsonLd).stream()
                .anyMatch(relation -> relation.range().stream().anyMatch(t -> appearsOnType(t, jsonLd)));
    }

    @Override
    public boolean appearsOnlyOnRecord(JsonLd jsonLd) {
        return hasDomainAdminMetadata(jsonLd);
    }

    @Override
    public List<String> domain() {
        return domain != null ? domain : List.of();
    }

    @Override
    public List<String> range() {
        return range != null ? range : List.of();
    }

    @Override
    public Map<String, Object> definition() {
        return definition;
    }

    public String name() {
        return name;
    }

    public boolean isRdfType() {
        return RDF_TYPE.equals(name);
    }

    public boolean isVocabTerm() {
        return isVocabTerm;
    }

    public boolean isXsdDate() {
        return range.contains("xsd:dateTime") || range.contains("xsd:date");
    }

    public boolean isDatatypeProperty() {
        return ((List<?>) asList(definition.get(TYPE_KEY))).stream().anyMatch(DATATYPE_PROPERTY::equals);
    }

    public boolean isInverseOf(Property property) {
        return inverseOf != null && inverseOf.equals(property.name());
    }

    public boolean isRestrictedSubProperty() {
        return definition.containsKey(SUB_PROPERTY_OF) && !objectOnPropertyRestrictions().isEmpty();
    }

    public boolean hasIndexKey() {
        return indexKey != null;
    }

    public List<Restrictions.OnProperty> objectOnPropertyRestrictions() {
        return objectOnPropertyRestrictions != null ? objectOnPropertyRestrictions : List.of();
    }

    protected List<Restrictions.OnProperty> getObjectHasValueRestrictions(JsonLd jsonLd) {
        return getObjectHasValueRestrictions(definition, jsonLd).stream()
                .map(Restrictions.OnProperty.class::cast)
                .toList();
    }

    public static List<Restrictions.HasValue> getObjectHasValueRestrictions(Map<String, Object> definition, JsonLd jsonLd) {
        List<Restrictions.HasValue> restrictions = new ArrayList<>();

        ((List<?>) asList(definition.get(RANGE))).stream()
                .map(Map.class::cast)
                .map(m -> (List<?>) m.get(SUBCLASS_OF))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(Map.class::cast)
                .filter(superClass -> RESTRICTION.equals(superClass.get(TYPE_KEY)))
                .forEach(restriction -> {
                    Optional<Property> onProperty = Optional.ofNullable(restriction.get(ON_PROPERTY))
                            .map(Property::getIri)
                            .map(jsonLd::toTermKey)
                            .map(pKey -> getProperty(pKey, jsonLd));
                    if (onProperty.isPresent()) {
                        Property p = onProperty.get();
                        Optional.ofNullable(restriction.get(HAS_VALUE))
                                .map(v -> {
                                    if (v instanceof String s) {
                                        return new Term(s);
                                    }
                                    var iri = getIri(v);
                                    if (iri != null) {
                                        if (p.isVocabTerm()) {
                                            var termKey = jsonLd.toTermKey(iri);
                                            return new VocabTerm(termKey, jsonLd.vocabIndex.get(termKey));
                                        }
                                        return new Link(iri);
                                    }
                                    throw new IllegalStateException("Unexpected value: " + v);
                                })
                                .map(hv -> new Restrictions.HasValue(p, hv))
                                .ifPresent(restrictions::add);
                    }
                });

        return restrictions;
    }

    protected Property getSuperProperty(JsonLd jsonLd) {
        return getProperty(getSuperKey(definition, jsonLd), jsonLd);
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

    private List<Selector> _getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        if (rdfSubjectTypes.isEmpty() || isPlatformTerm() || isRdfType()) {
            return List.of(this);
        }

        Set<Property> integralRelations = rdfSubjectTypes.stream()
                .map(t -> QueryUtil.getIntegralRelationsForType(t, jsonLd))
                .flatMap(List::stream)
                .collect(Collectors.toSet());

        Predicate<Property> followIntegralRelation = integralProp ->
                integralProp.range()
                        .stream()
                        .anyMatch(irRangeType -> this.mayAppearOnType(irRangeType, jsonLd));

        List<Selector> altSelectors = integralRelations.stream()
                .filter(followIntegralRelation)
                .map(ir -> new Path(List.of(ir, this)))
                .collect(Collectors.toList());

        if (altSelectors.isEmpty() || rdfSubjectTypes.stream().anyMatch(t -> this.mayAppearOnType(t, jsonLd))) {
            altSelectors.add(this);
        }

        /*
        FIXME:
         Integral relations are generally not applied to records.
         Bibliography is an exception: we need to search both meta.bibliography and hasInstance.meta.bibliography
         */
        if ("bibliography".equals(name)) {
            integralRelations.stream().filter(ir -> "hasInstance".equals(ir.name()))
                    .findFirst()
                    .map(hasInstance -> new Path(List.of(hasInstance, this)))
                    .ifPresent(altSelectors::add);
        }

        return altSelectors;
    }

    private boolean hasDomainAdminMetadata(JsonLd jsonLd) {
        return !domain.isEmpty() && domain.stream()
                .filter(d -> jsonLd.isSubClassOf(d, "AdminMetadata"))
                .count() == domain.size();
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

    private boolean isPlatformTerm() {
        // FIXME: don't hardcode
        return isCategory("https://id.kb.se/vocab/platform", definition);
    }

    private static boolean isComposite(Map<String, Object> definition) {
        // FIXME: don't hardcode
        return isCategory("https://id.kb.se/ns/librissearch/composite", definition);
    }

    private static boolean isShorthand(Map<String, Object> definition) {
        // FIXME: don't hardcode
        return isCategory("https://id.kb.se/vocab/shorthand", definition);
    }

    private static boolean isCategory(String categoryIri, Map<String, Object> definition) {
        return ((List<?>) asList(definition.get("category"))).stream()
                .anyMatch(c -> Map.of(ID_KEY, categoryIri).equals(c));
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

    protected static Stream<String> getTermKeys(List<?> terms, JsonLd jsonLd) {
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

        public NarrowedRestrictedProperty(String subPropertyKey, JsonLd jsonLd, Key.RecognizedKey queryKey) {
            super(subPropertyKey, jsonLd, queryKey);
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

        public String esField() {
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
        public String esField() {
            if (hasIndexKey()) {
                return indexKey;
            }
            if (superProperty != null) {
                return superProperty.esField();
            }
            throw new IllegalStateException();
        }

        @Override
        protected List<Restrictions.OnProperty> getObjectHasValueRestrictions(JsonLd jsonLd) {
            var range = getUnambiguousRange(jsonLd);
            if (range != null) {
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
                RdfType rdfType = new RdfType(jsonLd);
                VocabTerm value = new VocabTerm(range, jsonLd.vocabIndex.get(range));
                return List.of(new Restrictions.HasValue(rdfType, value));
            }
            return super.getObjectHasValueRestrictions(jsonLd);
        }

        @Override
        public String toString() {
            if (superProperty != null) {
                return superProperty.toString();
            }
            return "AnonymousProperty";
        }

        private String getUnambiguousRange(JsonLd jsonLd) {
            List<String> range = getTermKeys(asList(definition.get(RANGE)), jsonLd).toList();
            return range.size() == 1 ? range.getFirst() : null;
        }
    }

    private static class CompositeProperty extends Property {
        public CompositeProperty(String name, JsonLd jsonLd, Key.RecognizedKey key) {
            super(name, jsonLd, key);
        }

        @Override
        public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
            return getComponents(jsonLd).stream()
                    .flatMap(s -> s.getAltSelectors(jsonLd, rdfSubjectTypes).stream())
                    .toList();
        }

        private List<Selector> getComponents(JsonLd jsonLd) {
            List<Selector> components = new ArrayList<>();

            /*
            When multiple chain axioms are defined on the same property, the generated Json-LD ends up with an odd structure.

            For example:
            :p :propertyChainAxiom (:a :b), (:c, :d), (:e, :f) .

            -->
            
            {
                "propertyChainAxiom": [
                    { "@id": "https://id.kb.se/vocab/a },
                    { "@id": "https://id.kb.se/vocab/b },
                    [
                      { "@id": "https://id.kb.se/vocab/c" },
                      { "@id": "https://id.kb.se/vocab/d" }
                    ],
                    [
                      { "@id": "https://id.kb.se/vocab/e" },
                      { "@id": "https://id.kb.se/vocab/f" }
                    ]
                ]
            }

            The first chain is flattened into the top-level array, while subsequent chains are nested inside their own arrays.
            Probably a bug in definitions but for now we work around it here.
             */
            var chainDef = ((List<?>) definition.get(PROPERTY_CHAIN_AXIOM));
            var chain = getChain(chainDef, jsonLd);
            if (!chain.isEmpty()) {
                components.add(new Path(chain));
            }
            chainDef.stream().filter(List.class::isInstance)
                    .map(l -> getChain((List<?>) l, jsonLd))
                    .filter(Predicate.not(List::isEmpty))
                    .map(Path::new)
                    .forEach(components::add);

            jsonLd.getSubProperties(name)
                    .stream()
                    .map(p -> getProperty(p, jsonLd))
                    .forEach(components::add);

            return components;
        }

        private List<Selector> getChain(List<?> chainDef, JsonLd jsonLd) {
            return chainDef.stream()
                    .filter(Map.class::isInstance)
                    .map(QueryUtil::castToStringObjectMap)
                    .map(prop -> isLink(prop)
                            ? getProperty(jsonLd.toTermKey((String) prop.get(ID_KEY)), jsonLd)
                            : new AnonymousProperty(prop, jsonLd))
                    .map(Selector.class::cast)
                    .toList();
        }
    }

    private static class ShorthandProperty extends Property {
        private final Path propertyChain;

        public ShorthandProperty(String name, JsonLd jsonLd, Key.RecognizedKey key) {
            super(name, jsonLd, key);
            this.propertyChain = getPropertyChain(jsonLd);
        }

        @Override
        public Selector expand(JsonLd jsonLd) {
            return propertyChain.expand(jsonLd);
        }

        @Override
        public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
            return propertyChain.getAltSelectors(jsonLd, rdfSubjectTypes);
        }

        @Override
        public boolean isType() {
            return propertyChain.isType();
        }

        private Path getPropertyChain(JsonLd jsonLd) {
            // Expect only a single chain
            if (!(definition.get(PROPERTY_CHAIN_AXIOM) instanceof List<?> l && l.stream().allMatch(Map.class::isInstance))) {
                throw new RuntimeException("Invalid property chain axiom for shorthand property '" + name + "'.");
            }

            var chain = l.stream()
                    .map(QueryUtil::castToStringObjectMap)
                    .map(prop -> isLink(prop)
                            ? getProperty(jsonLd.toTermKey((String) prop.get(ID_KEY)), jsonLd)
                            : new AnonymousProperty(prop, jsonLd))
                    .map(Selector.class::cast)
                    .toList();

            return new Path(chain);
        }
    }
}

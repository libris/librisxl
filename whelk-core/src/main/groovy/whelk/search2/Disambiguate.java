package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.And;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.Path;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.VocabTerm;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.search2.QueryUtil.loadThing;
import static whelk.util.DocumentUtil.getAtPath;

public class Disambiguate {
    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = Set.of("label", "prefLabel", "altLabel", "code", "librisQueryCode");

    private Whelk whelk;
    private JsonLd jsonLd;
    private final Map<String, Map<String, Object>> vocab;
    private Map<String, List<String>> domainByProperty;
    private Map<String, List<String>> rangeByProperty;

    private Map<String, String> propertyAliasMappings;
    private Map<String, Set<String>> ambiguousPropertyAliases;
    private Map<String, String> classAliasMappings;
    private Map<String, Set<String>> ambiguousClassAliases;
    private Map<String, String> enumAliasMappings;
    private Map<String, Set<String>> ambiguousEnumAliases;

    private Set<String> integralRelations;

    public static final class Rdfs {
        public static final String RESOURCE = "Resource";
        public static final String RDF_TYPE = "rdf:type";

        private static final String DOMAIN = "domain";
        private static final String RANGE = "range";
        private static final String SUBCLASS_OF = "subClassOf";
        private static final String SUBPROPERTY_OF = "subPropertyOf";
        private static final String IS_DEFINED_BY = "isDefinedBy";
    }

    private static final class Owl {
        private static final String PROPERTY_CHAIN_AXIOM = "propertyChainAxiom";
        private static final String RESTRICTION = "Restriction";
        private static final String ON_PROPERTY = "onProperty";
        private static final String HAS_VALUE = "hasValue";
        private static final String EQUIVALENT_CLASS = "equivalentClass";
        private static final String EQUIVALENT_PROPERTY = "equivalentProperty";
        private static final String OBJECT_PROPERTY = "ObjectProperty";
        private static final String DATATYPE_PROPERTY = "DatatypeProperty";
    }

    public static Map<String, Object> freeTextDefinition = Collections.emptyMap();

    public Disambiguate(Whelk whelk) {
        this.whelk = whelk;
        this.jsonLd = whelk.getJsonld();
        this.vocab = jsonLd.vocabIndex;
        this.integralRelations = jsonLd.getCategoryMembers("integral");
        this.domainByProperty = new HashMap<>();
        this.rangeByProperty = new HashMap<>();
        setAliasMappings();
        // FIXME: This should probably not be a static variable...
        if (freeTextDefinition.isEmpty()) {
            freeTextDefinition = getDefinition("textQuery");
        }
    }

    // For test
    public Disambiguate(Map<String, Map<String, Object>> vocab) {
        this.vocab = vocab;
    }

    public Optional<String> mapToProperty(String alias) {
        return Optional.ofNullable(propertyAliasMappings.get(alias.toLowerCase()));
    }

    public Optional<String> mapToKbvClass(String alias) {
        return Optional.ofNullable(classAliasMappings.get(alias.toLowerCase()));
    }

    public Optional<String> mapToEnum(String alias) {
        return Optional.ofNullable(enumAliasMappings.get(alias.toLowerCase()));
    }

    public Set<String> getAmbiguousPropertyMapping(String alias) {
        return ambiguousPropertyAliases.getOrDefault(alias, Collections.emptySet());
    }

    public Set<String> getAmbiguousClassMapping(String alias) {
        return ambiguousClassAliases.getOrDefault(alias, Collections.emptySet());
    }

    public Set<String> getAmbiguousEnumMapping(String alias) {
        return ambiguousEnumAliases.getOrDefault(alias, Collections.emptySet());
    }

    public Map<String, Object> getDefinition(String termKey) {
        return vocab.getOrDefault(termKey, Collections.emptyMap());
    }

    public List<String> getDomain(String property) {
        if (!domainByProperty.containsKey(property)) {
            domainByProperty.put(property, findDomainOrRange(property, Rdfs.DOMAIN));
        }
        return domainByProperty.get(property);
    }

    public List<String> getRange(String property) {
        if (!rangeByProperty.containsKey(property)) {
            rangeByProperty.put(property, findDomainOrRange(property, Rdfs.RANGE));
        }
        return rangeByProperty.get(property);
    }

    public List<String> getIntegralRelationsForType(String type) {
        return integralRelations.stream()
                .filter(prop -> getDomain(prop).stream().anyMatch(domain -> jsonLd.isSubClassOf(type, domain)))
                .toList();
    }

    public Set<String> getSubclasses(String type) {
        return jsonLd.getSubClasses(type);
    }

    public Set<String> getSuperclasses(String type) {
        return getSuperclasses(type, jsonLd);
    }

    public Object getChip(String iri) {
        return loadThing(iri, whelk).map(jsonLd::toChip).orElse(Collections.emptyMap());
    }

    public boolean isVocabTerm(String property) {
        return jsonLd.isVocabTerm(property);
    }

    public boolean isType(String property) {
        return Rdfs.RDF_TYPE.equals(property) || jsonLd.getSubProperties(Rdfs.RDF_TYPE).contains(property);
    }

    public boolean isSubclassOf(String type, String baseType) {
        return jsonLd.isSubClassOf(type, baseType);
    }

    static public boolean isLdKey(String s) {
        return JsonLd.LD_KEYS.contains(s);
    }
    
    public Node expandChainAxiom(Property property) {
        return _expandChainAxiom(property);
    }

    private String getQueryCode(String property) {
        return (String) getDefinition(property).get("librisQueryCode");
    }

    private Optional<Map<?, ?>> getDefinition(Map<?, ?> link, Whelk whelk) {
        var iri = get(link, ID_KEY, "");
        if (iri.isEmpty()) {
            return Optional.empty();
        }
        var fromVocab = get(vocab, jsonLd.toTermKey(iri), Map.of());
        return fromVocab.isEmpty() ? loadThing(iri, whelk) : Optional.of(fromVocab);
    }

    private static boolean isKbvTerm(Map<?, ?> termDefinition) {
        return "https://id.kb.se/vocab/".equals(get(termDefinition, List.of(Rdfs.IS_DEFINED_BY, ID_KEY), ""));
    }

    private boolean isMarc(String termKey) {
        return termKey.startsWith("marc:");
    }

    private boolean isClass(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch(type -> jsonLd.isSubClassOf((String) type, "Class"));
    }

    private boolean isEnum(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream()
                .map(String.class::cast)
                .flatMap(type -> Stream.concat(getSuperclasses(type, jsonLd).stream(), Stream.of(type)))
                .map(jsonLd::getInRange)
                .flatMap(Set::stream)
                .filter(this::isProperty)
                .anyMatch(this::isVocabTerm);
    }

    private boolean isProperty(String termKey) {
        return Optional.ofNullable(vocab.get(termKey))
                .map(Disambiguate::isProperty)
                .orElse(false);
    }

    private static boolean isProperty(Map<?, ?> termDefinition) {
        return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
    }

    public static boolean isObjectProperty(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch(Owl.OBJECT_PROPERTY::equals);
    }

    private static boolean isDatatypeProperty(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch(Owl.DATATYPE_PROPERTY::equals);
    }

    private static List<?> getTypes(Map<?, ?> termDefinition) {
        return asList(termDefinition.get(JsonLd.TYPE_KEY));
    }

    public static String toPrefixed(String iri) {
        // TODO: get prefix mappings from context
        Map<String, String> nsToPrefix = new HashMap<>();
        nsToPrefix.put("https://id.kb.se/vocab/", "kbv:");
        nsToPrefix.put("http://id.loc.gov/ontologies/bibframe/", "bf:");
        nsToPrefix.put("http://purl.org/dc/terms/", "dc:");
        nsToPrefix.put("http://schema.org/", "sdo:");
        nsToPrefix.put("https://id.kb.se/term/sao/", "sao:");
        nsToPrefix.put("https://id.kb.se/marc/", "marc:");
        nsToPrefix.put("https://id.kb.se/term/saogf/", "saogf:");
        nsToPrefix.put("https://id.kb.se/term/barn/", "barn:");
        nsToPrefix.put("https://id.kb.se/term/barngf/", "barngf:");
        nsToPrefix.put("https://libris.kb.se/library/", "sigel:");
        nsToPrefix.put("https://id.kb.se/language/", "lang:");
        nsToPrefix.put(Document.getBASE_URI().toString(), "libris:");

        for (String ns : nsToPrefix.keySet()) {
            if (iri.startsWith(ns)) {
                return iri.replace(ns, nsToPrefix.get(ns));
            }
        }

        return iri;
    }

    public static String expandPrefixed(String s) {
        if (!s.contains(":")) {
            return s;
        }
        // TODO: get prefix mappings from context
        Map<String, String> nsToPrefix = new HashMap<>();
        nsToPrefix.put("https://id.kb.se/vocab/", "kbv:");
        nsToPrefix.put("http://id.loc.gov/ontologies/bibframe/", "bf:");
        nsToPrefix.put("http://purl.org/dc/terms/", "dc:");
        nsToPrefix.put("http://schema.org/", "sdo:");
        nsToPrefix.put("https://id.kb.se/term/sao/", "sao:");
        nsToPrefix.put("https://id.kb.se/marc/", "marc:");
        nsToPrefix.put("https://id.kb.se/term/saogf/", "saogf:");
        nsToPrefix.put("https://id.kb.se/term/barn/", "barn:");
        nsToPrefix.put("https://id.kb.se/term/barngf/", "barngf:");
        nsToPrefix.put("https://libris.kb.se/library/", "sigel:");
        nsToPrefix.put("https://id.kb.se/language/", "lang:");
        nsToPrefix.put(Document.getBASE_URI().toString(), "libris:");

        for (String ns : nsToPrefix.keySet()) {
            String prefix = nsToPrefix.get(ns);
            if (s.startsWith(prefix)) {
                return s.replace(prefix, ns);
            }
        }

        return s;
    }

    private Node _expandChainAxiom(Property property) {
        List<Node> pathValueList = new ArrayList<>();

        List<Object> path = new ArrayList<>();

        for (var prop : getAsList(property.definition(), Owl.PROPERTY_CHAIN_AXIOM)) {
            String propIri = get(prop, ID_KEY, "");
            if (!propIri.isEmpty()) {
                path.add(new Property(jsonLd.toTermKey(propIri), this));
                continue;
            }

            propIri = get(prop, List.of(Rdfs.SUBPROPERTY_OF, 0, ID_KEY), "");
            if (propIri.isEmpty()) {
                throw new RuntimeException("Failed to expand chain axiom for property " + property);
            }
            path.add(new Property(jsonLd.toTermKey(propIri), this));

            getAsList(prop, List.of(Rdfs.RANGE, "*", ID_KEY)).stream()
                    .map(rangeIri -> jsonLd.toTermKey((String) rangeIri))
                    .map(rangeKey ->
                            new PathValue(
                                    new Path(path).append(new Property(Rdfs.RDF_TYPE, this)),
                                    null,
                                    new VocabTerm(rangeKey, getDefinition(rangeKey))
                            )
                    )
                    .forEach(pathValueList::add);

            getAsList(prop, Rdfs.RANGE).stream()
                    .flatMap(r -> getAsList(r, Rdfs.SUBCLASS_OF).stream())
                    .filter(superClass -> Owl.RESTRICTION.equals(get(superClass, TYPE_KEY, "")))
                    .forEach(superClass -> {
                                var onPropertyIri = get(superClass, List.of(Owl.ON_PROPERTY, ID_KEY), "");
                                var hasValueIri = get(superClass, List.of(Owl.HAS_VALUE, ID_KEY), "");
                                if (!onPropertyIri.isEmpty() && !hasValueIri.isEmpty()) {
                                    var onPropertyKey = jsonLd.toTermKey(onPropertyIri);
                                    var hasValueKey = jsonLd.toTermKey(hasValueIri);
                                    var pathValue = new PathValue(
                                            new Path(path).append(new Property(onPropertyKey, this)),
                                            null,
                                            vocab.containsKey(hasValueKey)
                                                    ? new VocabTerm(hasValueKey, getDefinition(hasValueKey))
                                                    : new Link(hasValueIri, getDefinition(hasValueKey))
                                    );
                                    pathValueList.add(pathValue);
                                }
                            }
                    );
        }

        pathValueList.add(new PathValue(path, null, null));

        return pathValueList.size() == 1 ? pathValueList.getFirst() : new And(pathValueList);
    }

    private List<String> findDomainOrRange(String property, String domainOrRange) {
        var propertyDefinition = vocab.get(property);
        if (propertyDefinition == null) {
            return Collections.emptyList();
        }
        return findDomainOrRange(propertyDefinition, domainOrRange, whelk);
    }

    private List<String> findDomainOrRange(Map<?, ?> propertyDefinition, String domainOrRange, Whelk whelk) {
        return findDomainOrRange(new LinkedList<>(List.of(propertyDefinition)), domainOrRange, whelk, new HashSet<>());
    }

    private List<String> findDomainOrRange(LinkedList<Map<?, ?>> queue, String domainOrRange, Whelk whelk, Set<Map<?, ?>> seenDefs) {
        if (queue.isEmpty()) {
            return Collections.emptyList();
        }

        var propertyDefinition = queue.pop();

        seenDefs.add(propertyDefinition);

        List<String> domainOrRangeIris = getDomainOrRangeIris(propertyDefinition, domainOrRange);

        if (!domainOrRangeIris.isEmpty()) {
            return domainOrRangeIris.stream().map(jsonLd::toTermKey).filter(vocab::containsKey).toList();
        }

        queue.addAll(collectInheritable(propertyDefinition, whelk).stream().filter(Predicate.not(seenDefs::contains)).toList());

        return findDomainOrRange(queue, domainOrRange, whelk, seenDefs);
    }

    List<Map<?, ?>> collectInheritable(Map<?, ?> propertyDefinition, Whelk whelk) {
        List<Map<?, ?>> inheritable = new ArrayList<>();

        getAsList(propertyDefinition, Owl.EQUIVALENT_PROPERTY)
                .forEach(ep -> getDefinition((Map<?, ?>) ep, whelk).ifPresent(inheritable::add));

        getAsList(propertyDefinition, Rdfs.SUBPROPERTY_OF)
                .forEach(superProp -> getDefinition((Map<?, ?>) superProp, whelk).ifPresent(inheritable::add));

        return inheritable;
    }

    private List<String> getDomainOrRangeIris(Map<?, ?> propertyDefinition, String domainOrRange) {
        String p = propertyDefinition.containsKey(domainOrRange) ? domainOrRange : domainOrRange + "Includes";
        return get(propertyDefinition, List.of(p, "*", ID_KEY), List.of());
    }

    private static Set<String> getSuperclasses(String cls, JsonLd jsonLd) {
        List<String> superclasses = new ArrayList<>();
        jsonLd.getSuperClasses(cls, superclasses);
        return new HashSet<>(superclasses);
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(Object o, List<Object> path, T defaultTo) {
        return (T) getAtPath(o, path, defaultTo);
    }

    private static <T> T get(Object o, String key, T defaultTo) {
        return get(o, List.of(key), defaultTo);
    }

    private static List<?> getAsList(Object o, List<Object> path) {
        return asList(get(o, path, null));
    }

    private static List<?> getAsList(Object o, String key) {
        return getAsList(o, List.of(key));
    }

    private void setAliasMappings() {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();
        this.classAliasMappings = new TreeMap<>();
        this.ambiguousClassAliases = new TreeMap<>();
        this.enumAliasMappings = new TreeMap<>();
        this.ambiguousEnumAliases = new TreeMap<>();

        vocab.forEach((termKey, termDefinition) -> {
            if (isKbvTerm(termDefinition)) {
                if (isClass(termDefinition)) {
                    addAllMappings(termKey, classAliasMappings, ambiguousClassAliases);
                } else if (isProperty(termDefinition)) {
                    addAllMappings(termKey, propertyAliasMappings, ambiguousPropertyAliases);
                }
            }

            if (isMarc(termKey) && isProperty(termDefinition)) {
                addMapping(termKey, termKey, propertyAliasMappings, ambiguousPropertyAliases);
                addMapping((String) termDefinition.get(ID_KEY), termKey, propertyAliasMappings, ambiguousPropertyAliases);
            }

            if (isEnum(termDefinition)) {
                addAllMappings(termKey, enumAliasMappings, ambiguousEnumAliases);
            }

            if (Rdfs.RDF_TYPE.equals(termKey)) {
                addMapping(JsonLd.TYPE_KEY, termKey, propertyAliasMappings, ambiguousPropertyAliases);
                addAllMappings(termKey, propertyAliasMappings, ambiguousPropertyAliases);
            }
        });

        BiConsumer<Map<String, Set<String>>, Map<String, String>> disambiguateAliases = (ambiguousAliases, aliasMappings) ->
                ambiguousAliases.forEach((alias, mappedTerms) ->
                        mappedTerms.stream()
                                .filter(term -> alias.equals(term) || alias.toUpperCase().equals(getQueryCode(term)))
                                .forEach(term -> aliasMappings.put(alias, term))
                );

        disambiguateAliases.accept(ambiguousClassAliases, classAliasMappings);
        disambiguateAliases.accept(ambiguousPropertyAliases, propertyAliasMappings);
        disambiguateAliases.accept(ambiguousEnumAliases, enumAliasMappings);
    }

    private void addAllMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases) {
        addMapping(termKey, termKey, aliasMappings, ambiguousAliases);
        addMappings(termKey, aliasMappings, ambiguousAliases);
        addEquivTermMappings(termKey, aliasMappings, ambiguousAliases);
    }

    private void addMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases) {
        Map<String, Object> termDefinition = getDefinition(termKey);
        String termId = (String) termDefinition.get(ID_KEY);

        addMapping(termId, termKey, aliasMappings, ambiguousAliases);
        addMapping(toPrefixed(termId), termKey, aliasMappings, ambiguousAliases);

        for (String prop : notatingProps) {
            if (termDefinition.containsKey(prop)) {
                addMapping((String) termDefinition.get(prop), termKey, aliasMappings, ambiguousAliases);
            }

            String alias = (String) jsonLd.langContainerAlias.get(prop);
            if (termDefinition.containsKey(alias)) {
                for (String lang : jsonLd.locales) {
                    getAsList(termDefinition, List.of(alias, lang))
                            .forEach(langStr -> addMapping((String) langStr, termKey, aliasMappings, ambiguousAliases));
                }
            }
        }
    }

    private void addMapping(String from, String to, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases) {
        from = from.toLowerCase();
        if (ambiguousAliases.containsKey(from)) {
            ambiguousAliases.get(from).add(to);
        } else if (aliasMappings.containsKey(from)) {
            if (aliasMappings.get(from).equals(to)) {
                return;
            }
            ambiguousAliases.put(from, new HashSet<>(Set.of(to, aliasMappings.remove(from))));
        } else {
            aliasMappings.put(from, to);
        }
    }

    private void addEquivTermMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases) {
        String mappingProperty = isProperty(termKey) ? Owl.EQUIVALENT_PROPERTY : Owl.EQUIVALENT_CLASS;

        getAsList(vocab, List.of(termKey, mappingProperty)).forEach(term -> {
            String equivPropIri = get(term, ID_KEY, "");
            if (!equivPropIri.isEmpty()) {
                String equivPropKey = jsonLd.toTermKey(equivPropIri);
                if (!vocab.containsKey(equivPropKey)) {
                    loadThing(equivPropIri, whelk).ifPresentOrElse(
                            (equivPropDef) -> addMappings(termKey, aliasMappings, ambiguousAliases),
                            () -> {
                                addMapping(equivPropIri, termKey, aliasMappings, ambiguousAliases);
                                addMapping(toPrefixed(equivPropIri), termKey, aliasMappings, ambiguousAliases);
                            }
                    );
                }
            }
        });
    }
}

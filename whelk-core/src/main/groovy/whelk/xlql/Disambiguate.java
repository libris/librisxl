package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;
import java.util.stream.Collectors;

public class Disambiguate {
    public record PropertyChain(List<String> path, List<DefaultField> defaultFields) {
    }

    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));

    private final JsonLd jsonLd;
    private final Map<String, String> domainByProperty;

    private Map<String, String> propertyAliasMappings;
    // TODO: Handle ambiguous aliases
    private Map<String, Set<String>> ambiguousPropertyAliases;
    private Map<String, String> classAliasMappings;
    private Map<String, Set<String>> ambiguousClassAliases;
    private Map<String, String> enumAliasMappings;
    private Map<String, Set<String>> ambiguousEnumAliases;

    private Set<String> adminMetadataTypes;
    private Set<String> creationSuperTypes;
    public Set<String> workTypes;
    public Set<String> instanceTypes;

    private enum TermType {
        CLASS,
        PROPERTY,
        ENUM
    }

    public enum OutsetType {
        INSTANCE,
        WORK,
        RESOURCE
    }

    public enum DomainCategory {
        ADMIN_METADATA,
        WORK,
        INSTANCE,
        CREATION_SUPER,
        EMBODIMENT,
        UNKNOWN,
        OTHER
    }

    public static final String UNKNOWN_DOMAIN = "Unknown domain";

    public Disambiguate(Whelk whelk) {
        this.jsonLd = whelk.getJsonld();
        this.domainByProperty = loadDomainByProperty(whelk);
        setAliasMappings(whelk);
        setTypeSets(jsonLd);
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

    static public boolean isLdKey(String s) {
        return JsonLd.LD_KEYS.contains(s);
    }

    public String getDomain(String property) {
        return domainByProperty.getOrDefault(property, UNKNOWN_DOMAIN);
    }

    public OutsetType decideOutset(SimpleQueryTree sqt) {
        Set<OutsetType> outset = sqt.collectGivenTypes()
                .stream()
                .map(this::getOutsetType)
                .collect(Collectors.toSet());

        // TODO: Review this (for now default to Resource)
        return outset.size() == 1 ? outset.stream().findFirst().get() : OutsetType.RESOURCE;
    }

    private OutsetType getOutsetType(String type) {
        if (workTypes.contains(type)) {
            return OutsetType.WORK;
        }
        if (instanceTypes.contains(type)) {
            return OutsetType.INSTANCE;
        }
        return OutsetType.RESOURCE;
    }

    public DomainCategory getDomainCategory(String domain) {
        if (adminMetadataTypes.contains(domain)) {
            return DomainCategory.ADMIN_METADATA;
        }
        if (workTypes.contains(domain)) {
            return DomainCategory.WORK;
        }
        if (instanceTypes.contains(domain)) {
            return DomainCategory.INSTANCE;
        }
        if (creationSuperTypes.contains(domain)) {
            return DomainCategory.CREATION_SUPER;
        }
        if ("Embodiment".equals(domain)) {
            return DomainCategory.EMBODIMENT;
        }
        if (UNKNOWN_DOMAIN.equals(domain)) {
            return DomainCategory.UNKNOWN;
        }
        return DomainCategory.OTHER;
    }

    public boolean isVocabTerm(String property) {
        return jsonLd.isVocabTerm(property);
    }

    public PropertyChain expandChainAxiom(List<String> path) {
        List<String> extendedPath = new ArrayList<>();
        List<DefaultField> defaultFields = new ArrayList<>();

        for (String p : path) {
            var termDefinition = jsonLd.vocabIndex.get(p);

            // TODO: All short forms should be marked with :category :shortHand?
            //  Not the case at the moment, therefore isShorthand doesn't apply
//            if (!isShorthand(termDefinition)) {
            if (termDefinition == null || !termDefinition.containsKey("propertyChainAxiom")) {
                extendedPath.add(p);
                continue;
            }

            getAsList(termDefinition, "propertyChainAxiom").forEach(prop ->
                    {
                        getLinkedValue(prop)
                                .map(jsonLd::toTermKey)
                                .ifPresentOrElse(extendedPath::add,
                                        () -> getAsOptionalList(prop, JsonLd.SUB_PROPERTY_OF)
                                                .map(List::getFirst)
                                                .flatMap(Disambiguate::getLinkedValue)
                                                .map(jsonLd::toTermKey)
                                                .ifPresentOrElse(extendedPath::add,
                                                        () -> {
                                                            throw new RuntimeException("Failed to expand chain axiom for property " + p);
                                                        })
                                );

                        getAsList(prop, JsonLd.RANGE)
                                .forEach(r -> {
                                    getLinkedValue(r)
                                            .map(jsonLd::toTermKey)
                                            .filter(jsonLd.vocabIndex::containsKey)
                                            .map(cls -> new DefaultField(plusElem(extendedPath, JsonLd.TYPE_KEY), cls))
                                            .ifPresent(defaultFields::add);

                                    getAsList(r, "subClassOf")
                                            .stream()
                                            .filter(sc -> "Restriction".equals(sc.get(JsonLd.TYPE_KEY)))
                                            .forEach(sc ->
                                                    getAsOptionalMap(sc, "onProperty")
                                                            .flatMap(Disambiguate::getLinkedValue)
                                                            .map(jsonLd::toTermKey)
                                                            .flatMap(onProperty ->
                                                                    getAsOptionalMap(sc, "hasValue")
                                                                            .flatMap(Disambiguate::getLinkedValue)
                                                                            .map(value -> new DefaultField(plusElem(extendedPath, onProperty), value))
                                                            )
                                                            .ifPresent(defaultFields::add)
                                            );
                                });
                    }
            );
        }

        return new PropertyChain(extendedPath, defaultFields);
    }

    private void setTypeSets(JsonLd jsonLd) {
        this.adminMetadataTypes = plusElem(jsonLd.getSubClasses("AdminMetadata"), "AdminMetadata");
        this.creationSuperTypes = plusElem(getSuperclasses("Creation", jsonLd), "Creation");
        this.workTypes = plusElem(jsonLd.getSubClasses("Work"), "Work");
        this.instanceTypes = plusElem(jsonLd.getSubClasses("Instance"), "Instance");
    }

    private void setAliasMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();
        this.classAliasMappings = new TreeMap<>();
        this.ambiguousClassAliases = new TreeMap<>();
        this.enumAliasMappings = new TreeMap<>();
        this.ambiguousEnumAliases = new TreeMap<>();

        for (String termKey : jsonLd.vocabIndex.keySet()) {
            var termDefinition = jsonLd.vocabIndex.get(termKey);

            if (isKbvTerm(termDefinition)) {
                if (isClass(termDefinition)) {
                    addAllMappings(termDefinition, termKey, TermType.CLASS, whelk);
                } else if (isProperty(termDefinition)) {
                    addAllMappings(termDefinition, termKey, TermType.PROPERTY, whelk);
                }
            }

            if (isMarc(termKey) && isProperty(termDefinition)) {
                addMapping(termKey, termKey, TermType.PROPERTY);
                addMapping(jsonLd.toTermId(termKey), termKey, TermType.PROPERTY);
            }

            if (isEnum(termDefinition)) {
                addAllMappings(termDefinition, termKey, TermType.ENUM, whelk);
            }

            if ("rdf:type".equals(termKey)) {
                addMapping("@type", termKey, TermType.PROPERTY);
                addAllMappings(termDefinition, termKey, TermType.PROPERTY, whelk);
            }
        }
    }

    private void addAllMappings(Map<?, ?> termDefinition, String termKey, TermType termType, Whelk whelk) {
        addMapping(termKey, termKey, termType);
        addMappings(termDefinition, termKey, termType);
        addEquivTermMappings(termDefinition, termKey, termType, whelk);
    }

    private void addEquivTermMappings(Map<?, ?> termDefinition, String termKey, TermType termType, Whelk whelk) {
        String mappingProperty = switch (termType) {
            case CLASS, ENUM -> "equivalentClass";
            case PROPERTY -> "equivalentProperty";
        };

        getAsList(termDefinition, mappingProperty)
                .forEach(ep -> {
                            String equivPropId = getLinkedValue(ep).get();
                            String equivPropKey = jsonLd.toTermKey(equivPropId);

                            if (!jsonLd.vocabIndex.containsKey(equivPropKey)) {
                                loadThing(equivPropId, whelk).ifPresentOrElse(
                                        (equivPropDef) ->
                                                addMappings(equivPropDef, termKey, termType),
                                        () -> {
                                            addMapping(equivPropId, termKey, termType);
                                            addMapping(toPrefixed(equivPropId), termKey, termType);
                                        }
                                );
                            }
                        }
                );
    }

    private Map<String, String> loadDomainByProperty(Whelk whelk) {
        Map<String, String> domainByProperty = new TreeMap<>();
        jsonLd.vocabIndex.entrySet()
                .stream()
                .filter(e -> isKbvTerm(e.getValue()) && isProperty(e.getValue()))
                .forEach(e -> findDomain(e.getValue(), whelk)
                        .ifPresent(domain ->
                                domainByProperty.put(jsonLd.toTermKey(e.getKey()), jsonLd.toTermKey(domain))
                        )
                );
        return domainByProperty;
    }

    private Optional<String> findDomain(Map<?, ?> propertyDefinition, Whelk whelk) {
        return findDomain(new LinkedList<>(List.of(propertyDefinition)), whelk);
    }

    private Optional<String> findDomain(LinkedList<Map<?, ?>> queue, Whelk whelk) {
        if (queue.isEmpty()) {
            return Optional.empty();
        }

        var propertyDefinition = queue.pop();

        Optional<String> domain = getDomainIri(propertyDefinition);
        if (domain.isPresent()) {
            return domain;
        }

        queue.addAll(collectInheritable(propertyDefinition, whelk));

        return findDomain(queue, whelk);
    }

    List<Map<?, ?>> collectInheritable(Map<?, ?> propertyDefinition, Whelk whelk) {
        List<Map<?, ?>> inheritable = new ArrayList<>();

        getAsList(propertyDefinition, "equivalentProperty")
                .forEach(ep -> getDefinition(ep, whelk).ifPresent(inheritable::add));

        getAsOptionalList(propertyDefinition, "propertyChainAxiom")
                .map(List::getFirst)
                .flatMap(firstInChain -> getDefinition(firstInChain, whelk))
                .ifPresent(inheritable::add);

        getAsList(propertyDefinition, "subPropertyOf")
                .forEach(superProp -> getDefinition(superProp, whelk).ifPresent(inheritable::add));

        return inheritable;
    }

    private Optional<String> getDomainIri(Map<?, ?> propertyDefinition) {
        return getAsOptionalList(propertyDefinition, "domain")
                .map(List::getFirst)
                .flatMap(Disambiguate::getLinkedValue);
    }

    private Optional<Map<?, ?>> getDefinition(Map<?, ?> node, Whelk whelk) {
        return getLinkedValue(node)
                .flatMap(id -> {
                            var fromVocab = Optional.ofNullable((Map<?, ?>) jsonLd.vocabIndex.get(jsonLd.toTermKey(id)));
                            return fromVocab.isPresent() ? fromVocab : loadThing(id, whelk);
                        }
                );
    }

    public Optional<Map<?, ?>> loadThing(String id, Whelk whelk) {
        return Optional.ofNullable(whelk.loadData(id))
                .map(data -> data.get(JsonLd.GRAPH_KEY))
                .map(graph -> (Map<?, ?>) ((List<?>) graph).get(1));
    }

    private void addMappings(Map<?, ?> fromTermData, String toTermKey, TermType termType) {
        String fromTermId = (String) fromTermData.get(JsonLd.ID_KEY);

        addMapping(fromTermId, toTermKey, termType);
        addMapping(toPrefixed(fromTermId), toTermKey, termType);

        for (String prop : notatingProps) {
            if (fromTermData.containsKey(prop)) {
                addMapping((String) fromTermData.get(prop), toTermKey, termType);
            }

            String alias = (String) jsonLd.langContainerAlias.get(prop);

            if (fromTermData.containsKey(alias)) {
                Map<?, ?> byLang = (Map<?, ?>) fromTermData.get(alias);
                for (String lang : jsonLd.locales) {
                    List<?> values = JsonLd.asList(byLang.get(lang));
                    values.forEach(v -> addMapping((String) v, toTermKey, termType));
                }
            }
        }
    }

    private void addMapping(String from, String to, TermType termType) {
        from = from.toLowerCase();

        Map<String, String> aliasMappings = switch (termType) {
            case CLASS -> classAliasMappings;
            case PROPERTY -> propertyAliasMappings;
            case ENUM -> enumAliasMappings;
        };
        Map<String, Set<String>> ambiguousAliases = switch (termType) {
            case CLASS -> ambiguousClassAliases;
            case PROPERTY -> ambiguousPropertyAliases;
            case ENUM -> ambiguousEnumAliases;
        };

        if (ambiguousAliases.containsKey(from)) {
            ambiguousAliases.get(from).add(to);
        } else if (aliasMappings.containsKey(from)) {
            if (aliasMappings.get(from).equals(to)) {
                return;
            }
            ambiguousAliases.put(from, new HashSet<>(Arrays.asList(to, aliasMappings.remove(from))));
        } else {
            aliasMappings.put(from, to);
        }
    }

    private static boolean isKbvTerm(Map<?, ?> termDefinition) {
        return getAsOptionalMap(termDefinition, "isDefinedBy")
                .flatMap(Disambiguate::getLinkedValue)
                .filter("https://id.kb.se/vocab/"::equals)
                .isPresent();
    }

    private boolean isMarc(String termKey) {
        return termKey.startsWith("marc:");
    }

    private boolean isClass(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch(type -> jsonLd.isSubClassOf(type, "Class"));
    }

    private boolean isEnum(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream()
                .map(type -> plusElem(getSuperclasses(type, jsonLd), type))
                .flatMap(Set::stream)
                .map(jsonLd::getInRange)
                .flatMap(Set::stream)
                .filter(this::isProperty)
                .anyMatch(this::isVocabTerm);
    }

    private boolean isProperty(String termKey) {
        return Optional.ofNullable(jsonLd.vocabIndex.get(termKey))
                .map(Disambiguate::isProperty)
                .orElse(false);
    }

    private static boolean isProperty(Map<?, ?> termDefinition) {
        return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
    }

    public boolean isObjectProperty(String termKey) {
        return Optional.ofNullable(jsonLd.vocabIndex.get(termKey))
                .map(Disambiguate::isObjectProperty)
                .orElse(false);
    }

    private static boolean isObjectProperty(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch("ObjectProperty"::equals);
    }

    private static boolean isDatatypeProperty(Map<?, ?> termDefinition) {
        return getTypes(termDefinition).stream().anyMatch("DatatypeProperty"::equals);
    }

    private static List<String> getTypes(Map<?, ?> termDefinition) {
        return JsonLd.asList(termDefinition.get(JsonLd.TYPE_KEY));
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

        for (String ns : nsToPrefix.keySet()) {
            String prefix = nsToPrefix.get(ns);
            if (s.startsWith(prefix)) {
                return s.replace(prefix, ns);
            }
        }

        return s;
    }

    private static Set<String> getSuperclasses(String cls, JsonLd jsonLd) {
        List<String> superclasses = new ArrayList<>();
        jsonLd.getSuperClasses(cls, superclasses);
        return new HashSet<>(superclasses);
    }

    private static Set<String> plusElem(Set<String> set, String s) {
        return new HashSet<>(plusElem(new ArrayList<>(set), s));
    }

    private static List<String> plusElem(List<String> l, String s) {
        return concat(l, List.of(s));
    }

    private static List<String> concat(List<String> a, List<String> b) {
        List<String> l = new ArrayList<>();
        l.addAll(a);
        l.addAll(b);
        return l;
    }

    private static Optional<String> getLinkedValue(Map<?, ?> m) {
        return Optional.ofNullable((String) m.get(JsonLd.ID_KEY));
    }

    private static List<Map<?, ?>> getAsList(Map<?, ?> m, String property) {
        return getAsOptionalList(m, property).orElse(Collections.emptyList());
    }

    private static Optional<List<Map<?, ?>>> getAsOptionalList(Map<?, ?> m, String property) {
        return Optional.ofNullable((List<Map<?, ?>>) m.get(property));
    }

    private static Optional<Map<?, ?>> getAsOptionalMap(Map<?, ?> m, String property) {
        return Optional.ofNullable((Map<?, ?>) m.get(property));
    }

    private static boolean isShorthand(Map<?, ?> termDefinition) {
        return getAsOptionalMap(termDefinition, "category")
                .flatMap(Disambiguate::getLinkedValue)
                .filter("https://id.kb.se/vocab/shorthand"::equals)
                .isPresent();
    }
}

package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;

// TODO: Disambiguate values too (not only properties)
public class Disambiguate {
    public record PropertyChain(List<String> path, List<DefaultField> defaultFields) {
    }

    private JsonLd jsonLd;

    // :category :heuristicIdentifier too broad...?
    private Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));
    private Map<String, String> propertyAliasMappings;
    // TODO: Handle ambiguous aliases
    private Map<String, Set<String>> ambiguousPropertyAliases;
    private Map<String, String> domainByProperty;

    private Set<String> adminMetadataTypes;
    private Set<String> creationSuperTypes;
    public Set<String> workTypes;
    public Set<String> instanceTypes;

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
        setPropertyAliasMappings(whelk);
        this.domainByProperty = loadDomainByProperty(whelk);
        setTypeSets(jsonLd);
    }

    public String mapToKbvProperty(String alias) {
        return propertyAliasMappings.get(alias.toLowerCase());
    }

    static public boolean isLdKey(String s) {
        return JsonLd.LD_KEYS.contains(s);
    }

    public String getDomain(String property) {
        // TODO: @type not in vocab, needs special handling, hardcode for now
        if (property == JsonLd.TYPE_KEY) {
            return "Resource";
        }
        return domainByProperty.getOrDefault(property, UNKNOWN_DOMAIN);
    }

    public OutsetType getOutsetType(String type) {
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
        if (domain == "Embodiment") {
            return DomainCategory.EMBODIMENT;
        }
        if (domain == UNKNOWN_DOMAIN) {
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
            Map<String, Object> termDefinition = jsonLd.vocabIndex.get(p);

            // TODO: All short forms should be marked with :category :shortHand?
            //  Not the case at the moment, therefore isShorthand doesn't apply
//            if (!isShorthand(termDefinition)) {
            if (!termDefinition.containsKey("propertyChainAxiom")) {
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
        this.adminMetadataTypes = getSubtypes("AdminMetadata", jsonLd);
        this.creationSuperTypes = getSupertypes("Creation", jsonLd);
        this.workTypes = getSubtypes("Work", jsonLd);
        this.instanceTypes = getSubtypes("Instance", jsonLd);
    }

    private void setPropertyAliasMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();

        Map<String, Map> vocab = jsonLd.vocabIndex;

        // Hardcoding these for now...
        addMapping("@type", "@type");
        addMapping("type", "@type");
        addMapping("typ", "@type");
        addMapping("rdf:type", "@type");

        for (String termKey : vocab.keySet()) {
            Map termDefinition = vocab.get(termKey);

            if (isKbvTerm(termDefinition) && isProperty(termDefinition)) {
                addMapping(termKey, termKey);
                addMappings(termDefinition, termKey);

                getAsList(termDefinition, "equivalentProperty")
                        .forEach(ep -> {
                                    String equivPropId = getLinkedValue(ep).get();
                                    String equivPropKey = jsonLd.toTermKey(equivPropId);

                                    if (!vocab.containsKey(equivPropKey)) {
                                        loadThing(equivPropId, whelk).ifPresentOrElse(
                                                (equivPropDef) ->
                                                        addMappings(equivPropDef, termKey),
                                                () -> {
                                                    addMapping(equivPropId, termKey);
                                                    addMapping(toPrefixed(equivPropId), termKey);
                                                }
                                        );
                                    }
                                }
                        );
            }
        }
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

    private Optional<String> findDomain(Map propertyDefinition, Whelk whelk) {
        return findDomain(new LinkedList<>(List.of(propertyDefinition)), whelk);
    }

    private Optional<String> findDomain(LinkedList<Map> queue, Whelk whelk) {
        if (queue.isEmpty()) {
            return Optional.empty();
        }

        Map propertyDefinition = queue.pop();

        Optional<String> domain = getDomainIri(propertyDefinition);
        if (domain.isPresent()) {
            return domain;
        }

        queue.addAll(collectInheritable(propertyDefinition, whelk));

        return findDomain(queue, whelk);
    }

    List<Map> collectInheritable(Map propertyDefinition, Whelk whelk) {
        List<Map> inheritable = new ArrayList<>();

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

    private Optional<String> getDomainIri(Map propertyDefinition) {
        return getAsOptionalList(propertyDefinition, "domain")
                .map(List::getFirst)
                .flatMap(Disambiguate::getLinkedValue);
    }

    private Optional<Map> getDefinition(Map node, Whelk whelk) {
        return getLinkedValue(node)
                .map(id ->
                        Optional.ofNullable(jsonLd.vocabIndex.get(jsonLd.toTermKey(id)))
                                .orElse(loadThing(id, whelk).orElse(null))
                );
    }

    private Optional<Map> loadThing(String id, Whelk whelk) {
        return Optional.ofNullable(whelk.loadData(id))
                .map(data -> data.get(JsonLd.GRAPH_KEY))
                .map(graph -> (Map) ((List) graph).get(1));
    }

    private void addMappings(Map fromTermData, String toTermKey) {
        String fromTermId = (String) fromTermData.get(JsonLd.ID_KEY);

        addMapping(fromTermId, toTermKey);
        addMapping(toPrefixed(fromTermId), toTermKey);

        for (String prop : notatingProps) {
            if (fromTermData.containsKey(prop)) {
                addMapping((String) fromTermData.get(prop), toTermKey);
            }

            String alias = (String) jsonLd.langContainerAlias.get(prop);

            if (fromTermData.containsKey(alias)) {
                Map byLang = (Map) fromTermData.get(alias);
                for (String lang : jsonLd.locales) {
                    List<String> values = JsonLd.asList(byLang.get(lang));
                    values.forEach(v -> addMapping(v, toTermKey));
                }
            }
        }
    }

    private void addMapping(String from, String to) {
        from = from.toLowerCase();
        if (ambiguousPropertyAliases.containsKey(from)) {
            ambiguousPropertyAliases.get(from).add(to);
        } else if (propertyAliasMappings.containsKey(from)) {
            if (propertyAliasMappings.get(from).equals(to)) {
                return;
            }
            ambiguousPropertyAliases.put(from, new HashSet<>(Arrays.asList(to, propertyAliasMappings.remove(from))));
        } else {
            propertyAliasMappings.put(from, to);
        }
    }

    private static boolean isKbvTerm(Map termDefinition) {
        Map definedBy = (Map) termDefinition.get("isDefinedBy");
        return definedBy != null && definedBy.get("@id").equals("https://id.kb.se/vocab/");
    }

    private static boolean isProperty(Map termDefinition) {
        return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
    }

    public boolean isObjectProperty(String termKey) {
        Map termDefinition = jsonLd.vocabIndex.get(termKey);
        return isObjectProperty(termDefinition);
    }

    private static boolean isObjectProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.TYPE_KEY);
        return "ObjectProperty".equals(type);
    }

    private static boolean isDatatypeProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.TYPE_KEY);
        return "DatatypeProperty".equals(type);
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

    private static Set<String> getSupertypes(String cls, JsonLd jsonLd) {
        List<String> superClasses = new ArrayList<>();
        jsonLd.getSuperClasses(cls, superClasses);
        superClasses.add(cls);
        return new HashSet<>(superClasses);
    }

    private static Set<String> getSubtypes(String baseClass, JsonLd jsonLd) {
        Set<String> subtypes = jsonLd.getSubClasses(baseClass);
        subtypes.add(baseClass);
        return subtypes;
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

    private static Optional<String> getLinkedValue(Map m) {
        return Optional.ofNullable((String) m.get(JsonLd.ID_KEY));
    }

    private static List<Map> getAsList(Map m, String property) {
        return getAsOptionalList(m, property).orElse(Collections.emptyList());
    }

    private static Optional<List<Map>> getAsOptionalList(Map m, String property) {
        return Optional.ofNullable((List<Map>) m.get(property));
    }

    private static Optional<Map> getAsOptionalMap(Map m, String property) {
        return Optional.ofNullable((Map) m.get(property));
    }

    private static boolean isShorthand(Map termDefinition) {
        return getAsOptionalMap(termDefinition, "category")
                .flatMap(Disambiguate::getLinkedValue)
                .filter("https://id.kb.se/vocab/shorthand"::equals)
                .isPresent();
    }
}

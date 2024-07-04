package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.And;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.PathValue;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.VocabTerm;

import java.util.*;
import java.util.stream.Collectors;

public class Disambiguate {
    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));

    private final JsonLd jsonLd;
    private final Map<String, String> domainByProperty;

    private Map<String, String> propertyAliasMappings;
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

    public static final String RDF_TYPE = "rdf:type";

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

    public Set<String> getAmbiguousPropertyMapping(String alias) {
        return ambiguousPropertyAliases.getOrDefault(alias, Collections.emptySet());
    }

    public Set<String> getAmbiguousClassMapping(String alias) {
        return ambiguousClassAliases.getOrDefault(alias, Collections.emptySet());
    }

    public Set<String> getAmbiguousEnumMapping(String alias) {
        return ambiguousEnumAliases.getOrDefault(alias, Collections.emptySet());
    }

    static public boolean isLdKey(String s) {
        return JsonLd.LD_KEYS.contains(s);
    }

    public String getDomain(String property) {
        return domainByProperty.getOrDefault(property, UNKNOWN_DOMAIN);
    }

    public OutsetType decideOutset(QueryTree qt) {
        Set<OutsetType> outset = qt.collectGivenTypes()
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

    public boolean hasVocabValue(String property) {
        return isVocabTerm(property) || isType(property);
    }

    public boolean isVocabTerm(String property) {
        return jsonLd.isVocabTerm(property);
    }

    public boolean isType(String property) {
        return RDF_TYPE.equals(property) || jsonLd.getSubProperties(RDF_TYPE).contains(property);
    }

    public boolean isShortHand(String property) {
        var termDefinition = jsonLd.vocabIndex.get(property);

        // TODO: All short forms should be marked with :category :shortHand?
        //  Not the case at the moment, therefore the commented out block below doesn't apply
//        return getAsOptionalMap(termDefinition, "category")
//                .flatMap(Disambiguate::getLinkIri)
//                .filter("https://id.kb.se/vocab/shorthand"::equals)
//                .isPresent();

        return termDefinition != null && termDefinition.containsKey("propertyChainAxiom");
    }

    public Node expandChainAxiom(String property) {
        var termDefinition = jsonLd.vocabIndex.get(property);

        List<Node> pathValueList = new ArrayList<>();

        List<String> path = new ArrayList<>();

        for (Map<?, ?> prop : getAsListOfMaps(termDefinition, "propertyChainAxiom")) {
            var propKey = getLinkIri(prop).map(jsonLd::toTermKey);
            if (propKey.isPresent()) {
                path.add(propKey.get());
                continue;
            }

            propKey = getAsOptionalListOfMaps(prop, JsonLd.SUB_PROPERTY_OF)
                    .map(List::getFirst)
                    .flatMap(Disambiguate::getLinkIri)
                    .map(jsonLd::toTermKey);

            if (propKey.isEmpty()) {
                throw new RuntimeException("Failed to expand chain axiom for property " + property);
            }

            path.add(propKey.get());

            for (Map<?, ?> r : getAsListOfMaps(prop, JsonLd.RANGE)) {
                getLinkIri(r).map(jsonLd::toTermKey)
                        .filter(jsonLd.vocabIndex::containsKey)
                        .map(VocabTerm::new)
                        .map(cls -> new PathValue(plusElem(path, RDF_TYPE), cls))
                        .ifPresent(pathValueList::add);

                for (Map<?, ?> sc : getAsListOfMaps(r, "subClassOf")) {
                    if ("Restriction".equals(sc.get(JsonLd.TYPE_KEY))) {
                        var onProperty = getAsOptionalMap(sc, "onProperty")
                                .flatMap(Disambiguate::getLinkIri)
                                .map(jsonLd::toTermKey);
                        var hasValue = getAsOptionalMap(sc, "hasValue")
                                .flatMap(Disambiguate::getLinkIri)
                                .map(v -> jsonLd.vocabIndex.containsKey(jsonLd.toTermKey(v))
                                        ? new VocabTerm(v)
                                        : new Link(v)
                                );
                        if (onProperty.isPresent() && hasValue.isPresent()) {
                            pathValueList.add(new PathValue(plusElem(path, onProperty.get()), hasValue.get()));
                        }
                    }
                }
            }
        }

        pathValueList.add(new PathValue(path));

        return pathValueList.size() == 1 ? pathValueList.getFirst() : new And(pathValueList);
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

            if (RDF_TYPE.equals(termKey)) {
                addMapping(JsonLd.TYPE_KEY, termKey, TermType.PROPERTY);
                addAllMappings(termDefinition, termKey, TermType.PROPERTY, whelk);
            }
        }

        for (var m : ambiguousPropertyAliases.entrySet()) {
            var alias = m.getKey();
            var mappedProps = m.getValue();
            for (String prop : mappedProps) {
                if (getQueryCode(prop).filter(alias.toUpperCase()::equals).isPresent()) {
                    propertyAliasMappings.put(alias, prop);
                }
                if (alias.equals(prop.toLowerCase())) {
                    propertyAliasMappings.put(alias, prop);
                }
            }
        }

        for (var m : ambiguousClassAliases.entrySet()) {
            var alias = m.getKey();
            var mappedClasses = m.getValue();
            for (String cls : mappedClasses) {
                if (alias.equals(cls.toLowerCase())) {
                    classAliasMappings.put(alias, cls);
                }
            }
        }

        for (var m : ambiguousEnumAliases.entrySet()) {
            var alias = m.getKey();
            var mappedEnums = m.getValue();
            for (String e : mappedEnums) {
                if (alias.equals(e.toLowerCase())) {
                    enumAliasMappings.put(alias, e);
                }
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

        getAsListOfMaps(termDefinition, mappingProperty)
                .forEach(ep ->
                        getLinkIri(ep).ifPresent(equivPropId -> {
                            String equivPropKey = jsonLd.toTermKey(equivPropId);
                            if (!jsonLd.vocabIndex.containsKey(equivPropKey)) {
                                QueryUtil.loadThing(equivPropId, whelk).ifPresentOrElse(
                                        (equivPropDef) ->
                                                addMappings(equivPropDef, termKey, termType),
                                        () -> {
                                                addMapping(equivPropId, termKey, termType);
                                                addMapping(toPrefixed(equivPropId), termKey, termType);
                                        }
                                );
                            }
                        }));
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

        getAsListOfMaps(propertyDefinition, "equivalentProperty")
                .forEach(ep -> getDefinition(ep, whelk).ifPresent(inheritable::add));

        getAsOptionalListOfMaps(propertyDefinition, "propertyChainAxiom")
                .map(List::getFirst)
                .flatMap(firstInChain -> getDefinition(firstInChain, whelk))
                .ifPresent(inheritable::add);

        getAsListOfMaps(propertyDefinition, "subPropertyOf")
                .forEach(superProp -> getDefinition(superProp, whelk).ifPresent(inheritable::add));

        return inheritable;
    }

    public Optional<String> getQueryCode(String property) {
        return Optional.ofNullable((Map<?, ?>) jsonLd.vocabIndex.get(property))
                .map(propDef -> (String) propDef.get("librisQueryCode"));
    }

    private Optional<String> getDomainIri(Map<?, ?> propertyDefinition) {
        return getAsOptionalListOfMaps(propertyDefinition, "domain")
                .map(List::getFirst)
                .flatMap(Disambiguate::getLinkIri);
    }

    private Optional<Map<?, ?>> getDefinition(Map<?, ?> node, Whelk whelk) {
        return getLinkIri(node)
                .flatMap(id -> {
                            var fromVocab = Optional.ofNullable((Map<?, ?>) jsonLd.vocabIndex.get(jsonLd.toTermKey(id)));
                            return fromVocab.isPresent() ? fromVocab : QueryUtil.loadThing(id, whelk);
                        }
                );
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
                .flatMap(Disambiguate::getLinkIri)
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

    public boolean isProperty(String termKey) {
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

    private static Optional<String> getLinkIri(Map<?, ?> m) {
        return Optional.ofNullable((String) m.get(JsonLd.ID_KEY));
    }

    private static List<Map<?, ?>> getAsListOfMaps(Map<?, ?> m, String property) {
        return getAsOptionalListOfMaps(m, property).orElse(Collections.emptyList());
    }

    private static Optional<List<Map<?, ?>>> getAsOptionalListOfMaps(Map<?, ?> m, String property) {
        return Optional.ofNullable((List<Map<?, ?>>) m.get(property));
    }

    private static Optional<Map<?, ?>> getAsOptionalMap(Map<?, ?> m, String property) {
        return Optional.ofNullable((Map<?, ?>) m.get(property));
    }
}

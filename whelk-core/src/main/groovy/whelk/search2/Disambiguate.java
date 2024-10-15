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
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.VocabTerm;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Disambiguate {
    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));

    private Whelk whelk;
    private JsonLd jsonLd;
    private Map<String, Map<String, Object>> vocab;
    private Map<String, String> domainByProperty;

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

    public static final String UNKNOWN_DOMAIN = "Unknown domain";

    public static final String RDF_TYPE = "rdf:type";

    public static Map<String, Object> freeTextDefinition = Collections.emptyMap();

    public Disambiguate(Whelk whelk) {
        this.whelk = whelk;
        this.jsonLd = whelk.getJsonld();
        this.vocab = jsonLd.vocabIndex;
        this.domainByProperty = loadDomainByProperty(whelk);
        setAliasMappings(whelk);
        setTypeSets(jsonLd);
        // FIXME: This should probably not be a static variable...
        if (freeTextDefinition.isEmpty()) {
            freeTextDefinition = getDefinition("textQuery");
        }
    }

    // For test
    public Disambiguate(Map<String, Object> data) {
        // TODO: Load data to use for testing methods depending on this class
        if (data.containsKey("vocab")) {
            this.vocab = getVocab(data);
        }
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

    private String getDomain(String property) {
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

    public Property.DomainCategory getDomainCategory(String property) {
        String domain = getDomain(property);

        if (adminMetadataTypes.contains(domain)) {
            return Property.DomainCategory.ADMIN_METADATA;
        }
        if (workTypes.contains(domain)) {
            return Property.DomainCategory.WORK;
        }
        if (instanceTypes.contains(domain)) {
            return Property.DomainCategory.INSTANCE;
        }
        if (creationSuperTypes.contains(domain)) {
            return Property.DomainCategory.CREATION_SUPER;
        }
        if ("Embodiment".equals(domain)) {
            return Property.DomainCategory.EMBODIMENT;
        }
        if (UNKNOWN_DOMAIN.equals(domain)) {
            return Property.DomainCategory.UNKNOWN;
        }
        return Property.DomainCategory.OTHER;
    }

    public boolean isVocabTerm(String property) {
        return jsonLd.isVocabTerm(property);
    }

    public boolean isType(String property) {
        return RDF_TYPE.equals(property) || jsonLd.getSubProperties(RDF_TYPE).contains(property);
    }

    public Node expandChainAxiom(Property property) {
        List<Node> pathValueList = new ArrayList<>();

        List<Object> path = new ArrayList<>();

        for (Map<?, ?> prop : getAsListOfMaps(property.definition(), "propertyChainAxiom")) {
            var propKey = getLinkIri(prop).map(jsonLd::toTermKey);
            if (propKey.isPresent()) {
                path.add(new Property(propKey.get(), this));
                continue;
            }

            propKey = getAsOptionalListOfMaps(prop, JsonLd.SUB_PROPERTY_OF)
                    .map(List::getFirst)
                    .flatMap(Disambiguate::getLinkIri)
                    .map(jsonLd::toTermKey);

            if (propKey.isEmpty()) {
                throw new RuntimeException("Failed to expand chain axiom for property " + property);
            }

            path.add(new Property(propKey.get(), this));

            for (Map<?, ?> r : getAsListOfMaps(prop, JsonLd.RANGE)) {
                getLinkIri(r).map(jsonLd::toTermKey)
                        .filter(vocab::containsKey)
                        .ifPresent(term -> pathValueList.add(new PathValue(
                                new Path(path).append(new Property(RDF_TYPE, this)),
                                null,
                                new VocabTerm(term, getDefinition(term))
                        )));

                for (Map<?, ?> sc : getAsListOfMaps(r, "subClassOf")) {
                    if ("Restriction".equals(sc.get(JsonLd.TYPE_KEY))) {
                        var onProperty = getAsOptionalMap(sc, "onProperty")
                                .flatMap(Disambiguate::getLinkIri)
                                .map(jsonLd::toTermKey)
                                .map(p -> new Property(p, this));
                        var hasValue = getAsOptionalMap(sc, "hasValue")
                                .flatMap(Disambiguate::getLinkIri)
                                .map(iri -> {
                                            var termKey = jsonLd.toTermKey(iri);
                                            return vocab.containsKey(termKey)
                                                    ? new VocabTerm(termKey, getDefinition(termKey))
                                                    : new Link(iri, getChip(iri));
                                        }
                                );
                        if (onProperty.isPresent() && hasValue.isPresent()) {
                            pathValueList.add(new PathValue(new Path(path).append(onProperty.get()),null, hasValue.get()));
                        }
                    }
                }
            }
        }

        pathValueList.add(new PathValue(path, null, null));

        return pathValueList.size() == 1 ? pathValueList.getFirst() : new And(pathValueList);
    }

    private void setTypeSets(JsonLd jsonLd) {
        this.adminMetadataTypes = addString(jsonLd.getSubClasses("AdminMetadata"), "AdminMetadata");
        this.creationSuperTypes = addString(getSuperclasses("Creation", jsonLd), "Creation");
        this.workTypes = addString(jsonLd.getSubClasses("Work"), "Work");
        this.instanceTypes = addString(jsonLd.getSubClasses("Instance"), "Instance");
    }

    private void setAliasMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();
        this.classAliasMappings = new TreeMap<>();
        this.ambiguousClassAliases = new TreeMap<>();
        this.enumAliasMappings = new TreeMap<>();
        this.ambiguousEnumAliases = new TreeMap<>();

        for (String termKey : vocab.keySet()) {
            var termDefinition = vocab.get(termKey);

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
                            if (!vocab.containsKey(equivPropKey)) {
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
        vocab.entrySet()
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
        return findDomain(new LinkedList<>(List.of(propertyDefinition)), whelk, new HashSet<>());
    }

    private Optional<String> findDomain(LinkedList<Map<?, ?>> queue, Whelk whelk, Set<Map<?, ?>> seenDefs) {
        if (queue.isEmpty()) {
            return Optional.empty();
        }

        var propertyDefinition = queue.pop();

        seenDefs.add(propertyDefinition);

        Optional<String> domain = getDomainIri(propertyDefinition);
        if (domain.isPresent()) {
            return domain;
        }

        queue.addAll(collectInheritable(propertyDefinition, whelk).stream().filter(Predicate.not(seenDefs::contains)).toList());

        return findDomain(queue, whelk, seenDefs);
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

    private Optional<String> getQueryCode(String property) {
        return Optional.ofNullable((Map<?, ?>) vocab.get(property))
                .map(propDef -> (String) propDef.get("librisQueryCode"));
    }

    private Optional<String> getDomainIri(Map<?, ?> propertyDefinition) {
        return getAsOptionalListOfMaps(propertyDefinition, "domain")
                .map(List::getFirst)
                .flatMap(Disambiguate::getLinkIri);
    }

    public Object getChip(String iri) {
        return QueryUtil.loadThing(iri, whelk).map(jsonLd::toChip).orElse(Collections.emptyMap());
    }

    public Map<String, Object> getDefinition(String termKey) {
        return vocab.getOrDefault(termKey, Collections.emptyMap());
    }

    private Optional<Map<?, ?>> getDefinition(Map<?, ?> node, Whelk whelk) {
        return getLinkIri(node)
                .flatMap(id -> {
                            var fromVocab = Optional.ofNullable((Map<?, ?>) vocab.get(jsonLd.toTermKey(id)));
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
                .map(type -> addString(getSuperclasses(type, jsonLd), type))
                .flatMap(Set::stream)
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

    public Set<String> getSuperclasses(String cls) {
        return getSuperclasses(cls, jsonLd);
    }

    private static Set<String> getSuperclasses(String cls, JsonLd jsonLd) {
        List<String> superclasses = new ArrayList<>();
        jsonLd.getSuperClasses(cls, superclasses);
        return new HashSet<>(superclasses);
    }

    private static Set<String> addString(Set<String> set, String s) {
        return Stream.concat(set.stream(), Stream.of(s)).collect(Collectors.toSet());
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

    private static Map<String, Map<String, Object>> getVocab(Map<String, Object> data) {
        return ((Map<?, ?>) data.get("vocab")).entrySet()
                .stream()
                .collect(Collectors.toMap(e -> (String) e.getKey(), e -> QueryUtil.castToStringObjectMap(e.getValue())));
    }
}

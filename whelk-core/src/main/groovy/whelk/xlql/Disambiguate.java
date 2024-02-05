package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;

// TODO: Disambiguate values too (not only properties)
public class Disambiguate {
    private JsonLd jsonLd;

    // :category :heuristicIdentifier too broad...?
    private Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));
    private Map<String, String> propertyAliasMappings;
    // TODO: Handle ambiguous aliases
    private Map<String, Set<String>> ambiguousPropertyAliases;
    private Map<String, String> domainByProperty;

    private Set<String> adminMetadataTypes;
    private Set<String> workTypes;
    private Set<String> instanceTypes;
    private Set<String> instanceSuperTypes;

    public enum DomainGroup {
        ADMIN_METADATA,
        WORK,
        INSTANCE,
        INSTANCE_SUPER,
        OTHER
    }

    public static final String UNKNOWN_DOMAIN = "Unknown domain";

    public Disambiguate(Whelk whelk) {
        this.jsonLd = whelk.getJsonld();
        setPropertyAliasMappings(whelk);
        this.domainByProperty = loadDomainByProperty(whelk);
        setDomainGroups(jsonLd);
    }

    public String mapToKbvProperty(String alias) {
        return propertyAliasMappings.get(alias.toLowerCase());
    }

    public String getDomain(String property) {
        // TODO: @type not in vocab, needs special handling
        return domainByProperty.getOrDefault(property, UNKNOWN_DOMAIN);
    }

    public DomainGroup getDomainGroup(String domain) {
        if (adminMetadataTypes.contains(domain)) {
            return DomainGroup.ADMIN_METADATA;
        }
        if (workTypes.contains(domain)) {
            return DomainGroup.WORK;
        }
        if (instanceTypes.contains(domain)) {
            return DomainGroup.INSTANCE;
        }
        if (instanceSuperTypes.contains(domain)) {
            return DomainGroup.INSTANCE_SUPER;
        }
        return DomainGroup.OTHER;
    }

    // TODO: Handle owl:Restriction
    public List<String> expandChainAxiom(List<String> path) {
        List<String> extended = new ArrayList<>();

        for (String p : path) {
            Map<String, Object> termDefinition = jsonLd.getVocabIndex().get(p);

            if (!termDefinition.containsKey("propertyChainAxiom")) {
                extended.add(p);
                continue;
            }

            List<Map> pca = (List<Map>) termDefinition.get("propertyChainAxiom");
            for (Map prop : pca) {
                boolean added = false;

                if (prop.containsKey(JsonLd.getID_KEY())) {
                    String propId = (String) prop.get(JsonLd.getID_KEY());
                    added = extended.add(jsonLd.toTermKey(propId));
                } else if (prop.containsKey(JsonLd.getSUB_PROPERTY_OF())) {
                    List superProp = (List) prop.get(JsonLd.getSUB_PROPERTY_OF());
                    if (superProp.size() == 1) {
                        Map superPropLink = (Map) superProp.get(0);
                        if (superPropLink.containsKey(JsonLd.getID_KEY())) {
                            String superPropId = (String) superPropLink.get(JsonLd.getID_KEY());
                            added = extended.add(jsonLd.toTermKey(superPropId));
                        }
                    }
                }

                if (!added) {
                    throw new RuntimeException("Failed to expand chain axiom for property " + p);
                }
            }
        }

        return extended;
    }

    private void setDomainGroups(JsonLd jsonLd) {
        this.adminMetadataTypes = getSubtypes("AdminMetadata", jsonLd);
        this.workTypes = getSubtypes("Work", jsonLd);
        this.instanceTypes = getSubtypes("Instance", jsonLd);
        this.instanceSuperTypes = getSupertypes("Instance", jsonLd);
    }

    private void setPropertyAliasMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();

        Map<String, Map> vocab = jsonLd.getVocabIndex();

        // Hardcoding these for now...
        addMapping("type", "@type");
        addMapping("typ", "@type");
        addMapping("rdf:type", "@type");

        for (String termKey : vocab.keySet()) {
            Map termDefinition = vocab.get(termKey);
            if (isKbvTerm(termDefinition) && isProperty(termDefinition)) {
                addMapping(termKey, termKey);
                addMappings(termDefinition, termKey);
                if (termDefinition.containsKey("equivalentProperty")) {
                    List<Map> equivProperty = (List<Map>) termDefinition.get("equivalentProperty");
                    for (Map ep : equivProperty) {
                        String equivPropId = (String) ep.get(JsonLd.getID_KEY());
                        String equivPropKey = jsonLd.toTermKey(equivPropId);
                        if (!vocab.containsKey(equivPropKey)) {
                            Map equivPropData = whelk.loadData(equivPropKey);
                            if (equivPropData == null) {
                                addMapping(equivPropId, termKey);
                                addMapping(toPrefixed(equivPropId), termKey);
                            } else {
                                List graph = (List) equivPropData.get(JsonLd.getGRAPH_KEY());
                                Map equivPropDefinition = (Map) graph.get(1);
                                addMappings(equivPropDefinition, termKey);
                            }
                        }
                    }
                }
            }
        }
    }

    private Map<String, String> loadDomainByProperty(Whelk whelk) {
        Map<String, String> domainByProperty = new TreeMap<>();
        jsonLd.getVocabIndex().entrySet()
                .stream()
                .filter(e -> isKbvTerm(e.getValue()) && isProperty(e.getValue()))
                .forEach(e -> findDomain(e.getValue(), whelk)
                        .ifPresent(domain -> domainByProperty.put(jsonLd.toTermKey(e.getKey()), jsonLd.toTermKey(domain)))
                );
        return domainByProperty;
    }

    // TODO: BFS + review order
    private Optional<String> findDomain(Map propDefinition, Whelk whelk) {
        if (propDefinition.containsKey("domain")) {
            return Optional.of(propDefinition.get("domain"))
                    .map(d -> ((List) d).get(0))
                    .map(link -> (String) ((Map) link).get(JsonLd.getID_KEY()));
        } else if (propDefinition.containsKey("subPropertyOf")) {
            List<Map> subProperty = (List<Map>) propDefinition.get("subPropertyOf");
            for (Map sp : subProperty) {
                Optional<Map> spDefinition = getDefinition(sp, whelk);
                if (spDefinition.isPresent()) {
                    Optional<String> domain = findDomain(spDefinition.get(), whelk);
                    if (domain.isPresent()) {
                        return domain;
                    }
                }
            }
        } else if (propDefinition.containsKey("equivalentProperty")) {
            List<Map> equivProperty = (List<Map>) propDefinition.get("equivalentProperty");
            for (Map ep : equivProperty) {
                Optional<Map> epDefinition = getDefinition(ep, whelk);
                if (epDefinition.isPresent()) {
                    Optional<String> domain = findDomain(epDefinition.get(), whelk);
                    if (domain.isPresent()) {
                        return domain;
                    }
                }
            }
        } else if (propDefinition.containsKey("propertyChainAxiom")) {
            List pca = (List) propDefinition.get("propertyChainAxiom");
            Map first = (Map) pca.get(0);
            Optional<Map> pcaDefinition = getDefinition(first, whelk);
            if (pcaDefinition.isPresent()) {
                return findDomain(pcaDefinition.get(), whelk);
            }
        }

        return Optional.empty();
    }

    Optional<Map> getDefinition(Map object, Whelk whelk) {
        if (!object.containsKey(JsonLd.getID_KEY())) {
            return Optional.of(object);
        }
        String propId = (String) object.get(JsonLd.getID_KEY());
        String propKey = jsonLd.toTermKey(propId);
        Optional<Map> propDefinition = Optional.ofNullable(jsonLd.getVocabIndex().get(propKey));
        return propDefinition.isPresent()
                ? propDefinition
                : Optional.ofNullable(whelk.loadData(propId))
                .map(data -> data.get(JsonLd.getGRAPH_KEY()))
                .map(graph -> (Map) ((List) graph).get(1));
    }

    private void addMappings(Map fromTermData, String toTermKey) {
        String fromTermId = (String) fromTermData.get(JsonLd.getID_KEY());
        addMapping(fromTermId, toTermKey);
        addMapping(toPrefixed(fromTermId), toTermKey);
        for (String prop : notatingProps) {
            if (fromTermData.containsKey(prop)) {
                addMapping((String) fromTermData.get(prop), toTermKey);
            }
            String alias = (String) jsonLd.getLangContainerAlias().get(prop);
            if (fromTermData.containsKey(alias)) {
                Map byLang = (Map) fromTermData.get(alias);
                for (String lang : jsonLd.getLocales()) {
                    List values = JsonLd.asList(byLang.get(lang));
                    values.forEach(v -> addMapping((String) v, toTermKey));
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

    public static boolean isKbvTerm(Map termDefinition) {
        Map definedBy = (Map) termDefinition.get("isDefinedBy");
        return definedBy != null && definedBy.get("@id").equals("https://id.kb.se/vocab/");
    }

    public static boolean isProperty(Map termDefinition) {
        return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
    }

    public boolean isObjectProperty(String termKey) {
        Map termDefinition = jsonLd.getVocabIndex().get(termKey);
        return isObjectProperty(termDefinition);
    }

    private static boolean isObjectProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.getTYPE_KEY());
        return "ObjectProperty".equals(type);
    }

    private static boolean isDatatypeProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.getTYPE_KEY());
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

        for (String ns : nsToPrefix.keySet()) {
            String prefix = nsToPrefix.get(ns);
            if (s.startsWith(prefix)) {
                return s.replace(prefix, ns);
            }
        }

        return s;
    }

    private static Set<String> getSupertypes(String baseClass, JsonLd jsonLd) {
        List<String> superClasses = new ArrayList<>();
        jsonLd.getSuperClasses("Instance", superClasses);
        return new HashSet<>(superClasses);
    }

    private static Set<String> getSubtypes(String baseClass, JsonLd jsonLd) {
        Set<String> subtypes = jsonLd.getSubClasses(baseClass);
        subtypes.add(baseClass);
        return subtypes;
    }
}

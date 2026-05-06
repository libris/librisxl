package whelk.search2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.Term;
import whelk.search2.querytree.VocabTerm;
import whelk.util.DocumentUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.Owl.DATATYPE_PROPERTY;
import static whelk.JsonLd.Owl.EQUIVALENT_CLASS;
import static whelk.JsonLd.Owl.EQUIVALENT_PROPERTY;
import static whelk.JsonLd.Owl.OBJECT_PROPERTY;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.VOCAB_KEY;
import static whelk.JsonLd.asList;
import static whelk.search2.QueryUtil.loadThing;
import static whelk.util.DocumentUtil.getAtPath;

public record ResourceLookup(VocabMappings vocabMappings, ExternalMappings externalMappings) {
    private static final Logger logger = LogManager.getLogger(ResourceLookup.class);

    public static ResourceLookup load(Whelk whelk) {
        return new ResourceLookup(VocabMappings.load(whelk), ExternalMappings.load(whelk));
    }

    public ResourceLookup(VocabMappings vocabMappings) {
        this(vocabMappings, new ExternalMappings(Map.of()));
    }

    public record VocabMappings(
        /*
        Map<Code, Map<Namespace, Set<TermKey>>>
        for example:
            [
                "språk"    : ["https://id.kb.se/vocab/": ["language", "associatedLanguage"]],
                "bibliotek": ["librissearch": ["librissearch:itemHeldBy"]],
                "format"   : ["librissearch": ["librissearch:hasInstanceType"], "https://id.kb.se/vocab/": ["hasFormat", "format"]]
            ]
         */
            Map<String, Map<String, Set<String>>> properties,
        /*
        Map<Code, Map<Namespace, Set<TermKey>>>
        for example:
            [
                "person"         : ["https://id.kb.se/vocab/": ["Person"]],
                "digitalresource": ["https://id.kb.se/vocab/": ["DigitalResource"]]
            ]
         */
            Map<String, Map<String, Set<String>>> classes,
        /*
        Map<Code, Map<Namespace, Set<TermKey>>>
        for example:
            [
                "biblioteksnivå": ["marc": ["marc:MinimalLevel"]]
            ]
         */
            Map<String, Map<String, Set<String>>> enums,
        /*
        Map<BaseProperty, Map<Value, List<NarrowerProperty>>>
        for example:
        [
            "workCategory": [
                    "https://id.kb.se/term/ktg/Literature": ["findCategory"],
                    "https://id.kb.se/term/ktg/Software"  : ["findCategory"],
                    "https://id.kb.se/term/saogf/Poesi"   : ["identifyCategory"]
            ]
        ]
         */
            Map<String, Map<String, List<String>>> propertiesRestrictedByValue
    ) {
        // :category :heuristicIdentifier too broad...?
        private static final Set<String> notatingProps = Set.of("label", "prefLabel", "altLabel", "code", "librisQueryCode");

        static VocabMappings load(Whelk whelk) {
            return getMappings(whelk);
        }

        private static VocabMappings getMappings(Whelk whelk) {
            var jsonLd = whelk.getJsonld();
            var vocab = jsonLd.vocabIndex;
            var systemVocabNs = (String) whelk.getJsonld().context.get(VOCAB_KEY);

            Map<String, Map<String, Set<String>>> properties = new HashMap<>();
            Map<String, Map<String, Set<String>>> classes = new HashMap<>();
            Map<String, Map<String, Set<String>>> enums = new HashMap<>();

            List<String> coercingProperties = new ArrayList<>();

            vocab.forEach((termKey, termDefinition) -> {
                String ns = getNs(termKey, systemVocabNs);
                if (isProperty(termDefinition)) {
                    addAllMappings(termKey, ns, properties, whelk);
                    if (jsonLd.getCategoryMembers("librissearch:coercing").contains(termKey)) {
                        coercingProperties.add(termKey);
                    }
                } else if (isClass(termDefinition, jsonLd)) {
                    addAllMappings(termKey, ns, classes, whelk);
                } else if (isEnum(termDefinition, jsonLd)) {
                    addAllMappings(termKey, ns, enums, whelk);
                }
            });

            addMapping(JsonLd.TYPE_KEY, RDF_TYPE, "rdf", properties);

            return new VocabMappings(properties, classes, enums, getPropertiesRestrictedByValue(whelk, coercingProperties));
        }

        private static String getNs(String termKey, String systemVocabNs) {
            return JsonLd.looksLikeIri(termKey)
                    ? termKey.substring(termKey.lastIndexOf("/") + 1)
                    : (termKey.contains(":") ? termKey.substring(0, termKey.indexOf(":")) : systemVocabNs);
        }

        private static void addAllMappings(String termKey, String ns, Map<String, Map<String, Set<String>>> mappings, Whelk whelk) {
            addMapping(termKey, termKey, ns, mappings);
            addMappings(termKey, ns, mappings, whelk.getJsonld());
            addEquivTermMappings(termKey, ns, mappings, whelk);
        }

        private static void addMapping(String from, String to, String ns, Map<String, Map<String, Set<String>>> mappings) {
            mappings.computeIfAbsent(from.toLowerCase(), x -> new HashMap<>())
                    .computeIfAbsent(ns, x -> new HashSet<>())
                    .add(to);
        }

        private static void addMappings(String termKey, String ns, Map<String, Map<String, Set<String>>> mappings, JsonLd jsonLd) {
            addMappings(termKey, ns, mappings, jsonLd, jsonLd.vocabIndex.get(termKey));
        }

        private static void addMappings(String termKey, String ns, Map<String, Map<String, Set<String>>> mappings, JsonLd jsonLd, Map<String, Object> termDefinition) {
            String termId = (String) termDefinition.get(ID_KEY);

            addMapping(termId, termKey, ns, mappings);
            addMapping(toPrefixed(termId), termKey, ns, mappings);
            addMapping(dropNs(termId), termKey, ns, mappings);

            for (String prop : notatingProps) {
                if (termDefinition.containsKey(prop)) {
                    getAsList(termDefinition, List.of(prop))
                            .forEach(value -> addMapping((String) value, termKey, ns, mappings));
                }

                String alias = (String) jsonLd.langContainerAlias.get(prop);
                if (termDefinition.containsKey(alias)) {
                    for (String lang : jsonLd.locales) {
                        getAsList(termDefinition, List.of(alias, lang))
                                .forEach(langStr -> addMapping((String) langStr, termKey, ns, mappings));
                    }
                }
            }
        }

        private static String dropNs(String termIri) {
            return termIri.substring(termIri.lastIndexOf("/") + 1);
        }

        private static void addEquivTermMappings(String termKey, String ns, Map<String, Map<String, Set<String>>> mappings, Whelk whelk) {
            var jsonLd = whelk.getJsonld();
            var vocab = jsonLd.vocabIndex;

            String mappingProperty = isProperty(vocab.get(termKey)) ? EQUIVALENT_PROPERTY : EQUIVALENT_CLASS;

            getAsList(vocab, List.of(termKey, mappingProperty)).forEach(term -> {
                String equivPropIri = get(term, List.of(ID_KEY), "");
                if (!equivPropIri.isEmpty()) {
                    String equivPropKey = jsonLd.toTermKey(equivPropIri);
                    if (!vocab.containsKey(equivPropKey)) {
                        var thing = loadThing(equivPropIri, whelk);
                        if (!thing.isEmpty()) {
                            addMappings(termKey, ns, mappings, jsonLd, thing);
                        } else {
                            addMapping(equivPropIri, termKey, ns, mappings);
                            addMapping(toPrefixed(equivPropIri), termKey, ns, mappings);
                        }
                    }
                }
            });
        }

        private static boolean isClass(Map<String, Object> termDefinition, JsonLd jsonLd) {
            return getTypes(termDefinition).stream().anyMatch(type -> jsonLd.isSubClassOf((String) type, "Class"));
        }

        private static boolean isEnum(Map<String, Object> termDefinition, JsonLd jsonLd) {
            return getTypes(termDefinition).stream()
                    .map(String.class::cast)
                    .flatMap(type -> Stream.concat(jsonLd.getSuperClasses(type).stream(), Stream.of(type)))
                    .map(jsonLd::getInRange)
                    .flatMap(Set::stream)
                    .filter(s -> isProperty(jsonLd.vocabIndex.getOrDefault(s, Map.of())))
                    .anyMatch(jsonLd::isVocabTerm);
        }

        private static boolean isProperty(Map<String, Object> termDefinition) {
            return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
        }

        public static boolean isObjectProperty(Map<String, Object> termDefinition) {
            return getTypes(termDefinition).stream().anyMatch(OBJECT_PROPERTY::equals);
        }

        private static boolean isDatatypeProperty(Map<String, Object> termDefinition) {
            return getTypes(termDefinition).stream().anyMatch(DATATYPE_PROPERTY::equals);
        }

        private static List<?> getTypes(Map<?, ?> termDefinition) {
            return asList(termDefinition.get(JsonLd.TYPE_KEY));
        }

        private static Map<String, Map<String, List<String>>> getPropertiesRestrictedByValue(Whelk whelk, List<String> coercingProps) {
            JsonLd ld = whelk.getJsonld();

            Map<String, List<String>> groupedBySuperProp = new HashMap<>();
            coercingProps.forEach(coercing -> {
                String superProp = get(ld.vocabIndex, List.of(coercing, JsonLd.Rdfs.SUB_PROPERTY_OF, 0, ID_KEY), "");
                if (!superProp.isEmpty()) {
                    groupedBySuperProp.computeIfAbsent(ld.toTermKey(superProp), x -> new ArrayList<>())
                            .add(coercing);
                }
            });

            Map<String, Map<String, List<String>>> propertiesRestrictedByValue = new HashMap<>();

            groupedBySuperProp.forEach((superProp, coercing) -> {
                var types = new HashSet<String>();

                ld.getRange(superProp).forEach(type -> {
                    types.add(type);
                    types.addAll(ld.getSubClasses(type));
                });

                for (String type : types) {
                    for (var doc : whelk.getStorage().loadAllByType(type)) {
                        var iri = doc.getThingIdentifiers().stream().findFirst().orElseThrow();
                        for (var n : coercing) {
                            var propDef = ld.vocabIndex.getOrDefault(n, Map.of());
                            Property.buildObjectRestrictions(propDef, ld, true).forEach(hasValueRestriction -> {
                                var onProperty = hasValueRestriction.onProperty();
                                var hasValue = hasValueRestriction.value();
                                var path = List.of(JsonLd.GRAPH_KEY, 1, onProperty.esField());
                                Predicate<Object> hasMatchingValue = o -> switch (hasValue) {
                                    case Term term -> o instanceof String s && s.equals(term.term());
                                    case VocabTerm vocabTerm -> o instanceof String s && s.equals(vocabTerm.key());
                                    case Link link -> o instanceof Map<?,?> m && link.iri().equals(m.get(JsonLd.ID_KEY));
                                    default -> false;
                                };
                                var matches = ((List<?>) JsonLd.asList(DocumentUtil.getAtPath(doc.data, path, List.of()))).stream()
                                        .anyMatch(hasMatchingValue);
                                if (matches) {
                                    propertiesRestrictedByValue.computeIfAbsent(superProp, k -> new HashMap<>())
                                            .computeIfAbsent(iri, k -> new ArrayList<>())
                                            .add(n);
                                }
                            });
                        }
                    }
                }
            });

            return propertiesRestrictedByValue;
        }

        @SuppressWarnings("unchecked")
        private static <T> T get(Object o, List<Object> path, T defaultTo) {
            return (T) getAtPath(o, path, defaultTo);
        }

        private static List<?> getAsList(Object o, List<Object> path) {
            return asList(get(o, path, null));
        }
    }

    public record ExternalMappings(
            /*
            Map<Type, Map<Code, ResourceIri>>
            for example:
                [
                    "Library"           : ["s": ["https://libris.kb.se/library/S"]],
                    "bibdb:Organization": ["kb": ["https://libris.kb.se/library/org/KB"]]
                    "Country"           : ["sw": ["https://id.kb.se/country/sw"]]
                ]
            */
            Map<String, Map<String, Map<String, Object>>> byType
    ) {
        static ExternalMappings load(Whelk whelk) {
            return loadMappings(whelk);
        }

        // TODO: get from vocab
        private static ExternalMappings loadMappings(Whelk whelk) {
            Map<String, Map<String, Map<String, Object>>> mappings = new HashMap<>();
            mappings.put("Library", loadMappingsForType("Library", List.of("sigel"), whelk));
            mappings.put("Bibliography", loadMappingsForType("Bibliography", List.of("sigel"), whelk));
            mappings.put("bibdb:Organization", loadMappingsForType("bibdb:Organization", List.of("code"), whelk));
            mappings.put("Country", loadMappingsForType("Country", List.of("code"), whelk));
            mappings.put("IntendedAudience", loadMappingsForType("marc:AudienceType", List.of("code"), whelk));
            mappings.put("Language", loadMappingsForType("Language", List.of("langCode", "langCodeFull", "langCodeTerm", "langCodeShort", "langCodeLibrisLocal"), whelk));
            return new ExternalMappings(mappings);
        }

        private static Map<String, Map<String, Object>> loadMappingsForType(String type, Collection<String> properties, Whelk whelk) {
            Map<String, Map<String, Object>> mappings = new HashMap<>();
            whelk.loadAllByType(type)
                    .forEach(doc -> {
                        var description = doc.getThing();
                        properties.forEach(p -> {
                            if (description.containsKey(p) && description.get(p) instanceof String s) {
                                var chip = QueryUtil.castToStringObjectMap(whelk.jsonld.toChip(description));
                                mappings.put(s.toLowerCase(), chip);
                            }
                        });
                    });

            logger.info("Loaded {} mappings for {}", mappings.size(), type);
            return mappings;
        }
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
        nsToPrefix.put("https://id.kb.se/term/ktg/", "ktg:");
        nsToPrefix.put("https://id.kb.se/term/rda/", "idrda:");
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
        nsToPrefix.put("https://id.kb.se/term/ktg/", "ktg:");
        nsToPrefix.put("https://id.kb.se/term/rda/", "idrda:");
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
}

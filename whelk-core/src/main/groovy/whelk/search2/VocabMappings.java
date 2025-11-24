package whelk.search2;

import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search2.querytree.Property;
import whelk.util.DocumentUtil;
import whelk.util.Restrictions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            "category": [
                    "https://id.kb.se/term/ktg/Literature": ["findCategory", "identifyCategory"],
                    "https://id.kb.se/term/ktg/Software"  : ["findCategory"],
                    "https://id.kb.se/term/saogf/Poesi"   : ["identifyCategory"]
            ]
        ]
         */
        Map<String, Map<String, List<String>>> propertiesRestrictedByValue
) {
    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = Set.of("label", "prefLabel", "altLabel", "code", "librisQueryCode");

    public static VocabMappings load(Whelk whelk) {
        return getMappings(whelk);
    }

    private static VocabMappings getMappings(Whelk whelk) {
        var jsonLd = whelk.getJsonld();
        var vocab = jsonLd.vocabIndex;
        var systemVocabNs = (String) whelk.getJsonld().context.get(VOCAB_KEY);

        Map<String, Map<String, Set<String>>> properties = new HashMap<>();
        Map<String, Map<String, Set<String>>> classes = new HashMap<>();
        Map<String, Map<String, Set<String>>> enums = new HashMap<>();

        vocab.forEach((termKey, termDefinition) -> {
            String ns = getNs(termKey, systemVocabNs);
            if (isProperty(termDefinition)) {
                addAllMappings(termKey, ns, properties, whelk);
            } else if (isClass(termDefinition, jsonLd)) {
                addAllMappings(termKey, ns, classes, whelk);
            } else if (isEnum(termDefinition, jsonLd)) {
                addAllMappings(termKey, ns, enums, whelk);
            }
        });

        addMapping(JsonLd.TYPE_KEY, RDF_TYPE, "rdf", properties);

        return new VocabMappings(properties, classes, enums, getPropertiesRestrictedByValue(whelk));
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

    private static Map<String, Map<String, List<String>>> getPropertiesRestrictedByValue(Whelk whelk) {
        JsonLd ld = whelk.getJsonld();

        Map<String, List<String>> groupedBySuperProp = new HashMap<>();
        Restrictions.NARROWS.forEach((narrowerProp, superProp) ->
                groupedBySuperProp.computeIfAbsent(superProp, x -> new ArrayList<>())
                        .add(narrowerProp));

        Map<String, Map<String, List<String>>> propertiesRestrictedByValue = new HashMap<>();

        groupedBySuperProp.forEach((superProp, narrowerProps) -> {
            var types = new HashSet<String>();

            ld.getRange(superProp).forEach(type -> {
                types.add(type);
                types.addAll(ld.getSubClasses(type));
            });

            for (String type : types) {
                for (var doc : whelk.getStorage().loadAllByType(type)) {
                    var iri = doc.getThingIdentifiers().stream().findFirst().orElseThrow();
                    for (var n : narrowerProps) {
                        var propDef = ld.vocabIndex.getOrDefault(n, Map.of());
                        Property.getObjectHasValueRestrictions(propDef).forEach((onProperty, values) -> {
                            var path = List.of(JsonLd.GRAPH_KEY, 1, ld.toTermKey(onProperty));
                            @SuppressWarnings("unchecked")
                            var things = ((List<Map<?, ?>>) JsonLd.asList(DocumentUtil.getAtPath(doc.data, path, List.of())));
                            boolean matches = things.stream()
                                    .map(m -> (String) m.get(JsonLd.ID_KEY))
                                    .anyMatch(values::contains);
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
}

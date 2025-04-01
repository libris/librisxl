package whelk.search2;

import groovy.transform.PackageScope;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.Owl.DATATYPE_PROPERTY;
import static whelk.JsonLd.Owl.EQUIVALENT_CLASS;
import static whelk.JsonLd.Owl.EQUIVALENT_PROPERTY;
import static whelk.JsonLd.Owl.OBJECT_PROPERTY;
import static whelk.JsonLd.Rdfs.IS_DEFINED_BY;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.VOCAB_KEY;
import static whelk.JsonLd.asList;
import static whelk.search2.QueryUtil.loadThing;
import static whelk.util.DocumentUtil.getAtPath;

public class VocabMappings {
    // :category :heuristicIdentifier too broad...?
    private static final Set<String> notatingProps = Set.of("label", "prefLabel", "altLabel", "code", "librisQueryCode");

    public Map<String, String> propertyAliasMappings;
    public Map<String, Set<String>> ambiguousPropertyAliases;
    public Map<String, String> classAliasMappings;
    public Map<String, Set<String>> ambiguousClassAliases;
    public Map<String, String> enumAliasMappings;
    public Map<String, Set<String>> ambiguousEnumAliases;

    public VocabMappings(Whelk whelk) {
        setAliasMappings(whelk);
    }

    // For test
    @PackageScope
    public VocabMappings(Map<String, Object> mappings) {
        this.propertyAliasMappings = get(mappings, "propertyAliasMappings", new HashMap<>());
        this.classAliasMappings = get(mappings, "classAliasMappings", new HashMap<>());
        this.enumAliasMappings = get(mappings, "enumAliasMappings", new HashMap<>());
        this.ambiguousPropertyAliases = get(mappings, "ambiguousPropertyAliases", new HashMap<>());
        this.ambiguousClassAliases = get(mappings, "ambiguousClassAliases", new HashMap<>());
        this.ambiguousEnumAliases = get(mappings, "ambiguousEnumAliases", new HashMap<>());
    }

    private void setAliasMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();
        this.classAliasMappings = new TreeMap<>();
        this.ambiguousClassAliases = new TreeMap<>();
        this.enumAliasMappings = new TreeMap<>();
        this.ambiguousEnumAliases = new TreeMap<>();

        var jsonLd = whelk.getJsonld();
        var vocab = jsonLd.vocabIndex;

        vocab.forEach((termKey, termDefinition) -> {
            if (isAbstract(termDefinition)) {
                return;
            }
            if (isSystemVocabTerm(termDefinition, jsonLd)) {
                if (isClass(termDefinition, jsonLd)) {
                    addAllMappings(termKey, classAliasMappings, ambiguousClassAliases, whelk);
                } else if (isProperty(termDefinition)) {
                    addAllMappings(termKey, propertyAliasMappings, ambiguousPropertyAliases, whelk);
                }
            }

            if (isMarc(termKey) && isProperty(termDefinition)) {
                addMapping(termKey, termKey, propertyAliasMappings, ambiguousPropertyAliases);
                addMapping((String) termDefinition.get(ID_KEY), termKey, propertyAliasMappings, ambiguousPropertyAliases);
            }

            if (isEnum(termDefinition, jsonLd)) {
                addAllMappings(termKey, enumAliasMappings, ambiguousEnumAliases, whelk);
            }

            if (RDF_TYPE.equals(termKey)) {
                addMapping(JsonLd.TYPE_KEY, termKey, propertyAliasMappings, ambiguousPropertyAliases);
                addAllMappings(termKey, propertyAliasMappings, ambiguousPropertyAliases, whelk);
            }
        });
    }

    private void addAllMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases, Whelk whelk) {
        addMapping(termKey, termKey, aliasMappings, ambiguousAliases);
        addMappings(termKey, aliasMappings, ambiguousAliases, whelk.getJsonld());
        addEquivTermMappings(termKey, aliasMappings, ambiguousAliases, whelk);
    }

    private void addMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases, JsonLd jsonLd) {
        addMappings(termKey, aliasMappings, ambiguousAliases, jsonLd, jsonLd.vocabIndex.get(termKey));
    }

    private void addMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases, JsonLd jsonLd, Map<String, Object> termDefinition) {
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

    private void addEquivTermMappings(String termKey, Map<String, String> aliasMappings, Map<String, Set<String>> ambiguousAliases, Whelk whelk) {
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
                        addMappings(termKey, aliasMappings, ambiguousAliases, jsonLd, thing);
                    } else {
                        addMapping(equivPropIri, termKey, aliasMappings, ambiguousAliases);
                        addMapping(toPrefixed(equivPropIri), termKey, aliasMappings, ambiguousAliases);
                    }
                }
            }
        });
    }

    private static boolean isSystemVocabTerm(Map<String, Object> termDefinition, JsonLd jsonLd) {
        return get(termDefinition, List.of(IS_DEFINED_BY, ID_KEY), "")
                .equals(jsonLd.context.get(VOCAB_KEY));
    }

    private static boolean isMarc(String termKey) {
        return termKey.startsWith("marc:");
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

    private static boolean isAbstract(Map<String, Object> termDefinition) {
        return (boolean) termDefinition.getOrDefault("abstract", false);
    }

    private static List<?> getTypes(Map<?, ?> termDefinition) {
        return asList(termDefinition.get(JsonLd.TYPE_KEY));
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
